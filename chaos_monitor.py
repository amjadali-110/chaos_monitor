#!/usr/bin/env python3

import os
import json
import requests
import gzip
import zipfile
import logging
import shutil
import threading
from datetime import datetime
from pathlib import Path
from typing import Set, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter, Retry
import time

# Configuration
CHUNK_SIZE = 1024 * 64  # 64KB
DEFAULT_WORKERS = 5
TIMEOUT = 30
RETRIES = 3
RETRY_BACKOFF_FACTOR = 1

class Config:
    def __init__(self):
        # Try to load from .env file if it exists
        env_file = Path('.env')
        if env_file.exists():
            with open(env_file, 'r') as f:
                for line in f:
                    if '=' in line and not line.strip().startswith('#'):
                        key, value = line.strip().split('=', 1)
                        os.environ[key] = value
        
        self.chaos_index_url = os.getenv('CHAOS_INDEX_URL', 'https://chaos-data.projectdiscovery.io/index.json')
        self.download_dir = Path(os.getenv('DOWNLOAD_DIR', 'downloads'))
        self.latest_dir = self.download_dir / 'latest'
        self.previous_dir = self.download_dir / 'previous' 
        self.log_file = os.getenv('LOG_FILE', 'chaos_monitor.log')
        self.telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.workers = int(os.getenv('WORKERS', str(DEFAULT_WORKERS)))
        self.request_timeout = int(os.getenv('REQUEST_TIMEOUT', str(TIMEOUT)))
        self.max_retries = int(os.getenv('MAX_RETRIES', str(RETRIES)))

class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
    
    def send_message(self, message: str) -> bool:
        """Send a message to Telegram chat"""
        if not self.bot_token or not self.chat_id:
            logging.warning("Telegram credentials not configured")
            return False
        
        try:
            url = f"{self.base_url}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            logging.info("Telegram message sent successfully")
            return True
        except Exception as e:
            logging.error(f"Failed to send Telegram message: {e}")
            return False

class ChaosDownloader:
    """Downloads chaos files concurrently with retry logic"""
    
    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
        
        # Setup retry strategy
        retries = Retry(
            total=config.max_retries,
            backoff_factor=RETRY_BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.lock = threading.Lock()
        self.results = []
        
        # Setup logging
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def sanitize_filename(self, name: str) -> str:
        """Make a safe filename from program name"""
        import re
        from urllib.parse import unquote
        # Remove URL encoding
        name = unquote(name)
        # remove control chars
        name = re.sub(r'[\x00-\x1f\x7f]+', '', name)
        # keep letters, numbers, dash, underscore, dot
        name = re.sub(r'[^\w\-.() ]+', '_', name)
        # collapse multiple underscores/spaces
        name = re.sub(r'[ \t]+', '_', name).strip('_')
        name = re.sub(r'__+', '_', name)
        return name if name else "file"
    
    def platform_name(self, entry: Dict) -> str:
        """Get platform name, defaulting to self-hosted if empty"""
        platform = entry.get("platform")
        if platform is None:
            platform = ""
        platform = str(platform).strip()
        if platform == "":
            return "self-hosted"
        return self.sanitize_filename(platform)
    
    def choose_subfolder(self, entry: Dict) -> str:
        """Decide between Swag / Bounty / Non-Bounty with priority: swag > bounty > non-bounty"""
        swag = entry.get("swag", False)
        bounty = entry.get("bounty", False)
        if swag:
            return "Swag"
        if bounty:
            return "Bounty"
        return "Non-Bounty"
    
    def prepare_dest(self, platform: str, subfolder: str) -> Path:
        """Create destination directory with platform/subfolder structure"""
        dest_dir = self.config.latest_dir / platform / subfolder
        dest_dir.mkdir(parents=True, exist_ok=True)
        return dest_dir
    
    def download_entry(self, entry: Dict) -> Dict:
        """Download a single chaos entry"""
        name = entry.get('name', 'unknown')
        url = entry.get('URL', '')
        
        if not url:
            return {"entry": name, "success": False, "message": "No URL found"}
        
        # Determine platform and subfolder
        platform = self.platform_name(entry)
        subfolder = self.choose_subfolder(entry)
        dest_dir = self.prepare_dest(platform, subfolder)
        
        # Create filename
        filename = self.sanitize_filename(name)
        if not filename.lower().endswith('.zip'):
            filename += '.zip'
        
        dest_path = dest_dir / filename
        temp_path = dest_dir / (filename + '.part')
        
        # Skip if file already exists and has content
        if dest_path.exists() and dest_path.stat().st_size > 0:
            return {"entry": name, "success": True, "message": f"Already exists: {platform}/{subfolder}/{filename}"}
        
        try:
            with self.session.get(url, stream=True, timeout=self.config.request_timeout) as r:
                r.raise_for_status()
                
                total_size = r.headers.get('Content-Length')
                total_bytes = int(total_size) if total_size and total_size.isdigit() else None
                
                downloaded = 0
                with open(temp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                
                # Atomic rename
                temp_path.rename(dest_path)
                
                msg = f"Downloaded: {platform}/{subfolder}/{filename} ({downloaded} bytes)"
                return {"entry": name, "success": True, "message": msg}
                
        except Exception as e:
            # Cleanup temp file
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except:
                pass
            return {"entry": name, "success": False, "message": f"Failed: {str(e)}"}
    
    def download_all(self, entries: List[Dict]) -> Dict:
        """Download all entries concurrently"""
        self.logger.info(f"Starting downloads: {len(entries)} files with {self.config.workers} workers")
        
        # Create latest directory (subdirectories will be created as needed)
        self.config.latest_dir.mkdir(parents=True, exist_ok=True)
        
        # Download concurrently
        with ThreadPoolExecutor(max_workers=self.config.workers) as executor:
            future_map = {executor.submit(self.download_entry, entry): entry for entry in entries}
            
            for future in as_completed(future_map):
                entry = future_map[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = {"entry": entry.get("name", ""), "success": False, "message": f"Exception: {e}"}
                
                with self.lock:
                    self.results.append(result)
                    status = "OK" if result["success"] else "ERR"
                    self.logger.info(f"[{status}] {result['entry']}: {result['message']}")
        
        # Summary
        successful = sum(1 for r in self.results if r["success"])
        failed = sum(1 for r in self.results if not r["success"])
        
        self.logger.info(f"Download complete: {successful} success, {failed} failed")
        
        return {
            "total": len(self.results),
            "successful": successful,
            "failed": failed,
            "results": self.results
        }

class SubdomainExtractor:
    """Extracts subdomains from zip files"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def extract_subdomains_from_zip(self, zip_path: Path) -> Set[str]:
        """Extract all subdomains from a zip file"""
        subdomains = set()
        
        if not zip_path.exists():
            self.logger.warning(f"Zip file does not exist: {zip_path}")
            return subdomains
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_files = zip_ref.namelist()
                
                for zip_filename in zip_files:
                    if zip_filename.endswith('.txt') or zip_filename.endswith('.csv') or '.' not in zip_filename:
                        try:
                            with zip_ref.open(zip_filename) as f:
                                import io
                                with io.TextIOWrapper(f, encoding='utf-8', errors='ignore') as text_f:
                                    for line in text_f:
                                        subdomain = line.strip()
                                        if subdomain and not subdomain.startswith('#'):
                                            subdomains.add(subdomain)
                        except Exception as e:
                            self.logger.warning(f"Failed to process {zip_filename} in {zip_path.name}: {e}")
                            
        except Exception as e:
            self.logger.error(f"Failed to extract from {zip_path.name}: {e}")
        
        return subdomains
    
    def extract_all_subdomains(self, directory: Path) -> Dict[str, Set[str]]:
        """Extract subdomains from all zip files in a nested directory structure"""
        results = {}
        
        if not directory.exists():
            self.logger.warning(f"Directory does not exist: {directory}")
            return results
        
        # Find all zip files recursively in the nested structure
        zip_files = list(directory.rglob('*.zip'))
        self.logger.info(f"Extracting subdomains from {len(zip_files)} files in {directory.name}")
        
        for zip_path in zip_files:
            program_name = zip_path.stem  # filename without .zip extension
            subdomains = self.extract_subdomains_from_zip(zip_path)
            results[program_name] = subdomains
            self.logger.debug(f"Extracted {len(subdomains)} subdomains from {program_name}")
        
        return results

class ChaosMonitor:
    """Main chaos monitoring class"""
    
    def __init__(self, config: Config):
        self.config = config
        self.telegram = TelegramNotifier(config.telegram_bot_token, config.telegram_chat_id)
        self.downloader = ChaosDownloader(config)
        self.extractor = SubdomainExtractor()
        
        # Create directories
        self.config.download_dir.mkdir(exist_ok=True)
        self.config.latest_dir.mkdir(exist_ok=True)
        self.config.previous_dir.mkdir(exist_ok=True)
        
        # Setup logging with UTF-8 encoding
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(config.log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def fetch_chaos_index(self) -> Optional[List[Dict]]:
        """Fetch the chaos index JSON"""
        try:
            self.logger.info("Fetching chaos index...")
            response = requests.get(self.config.chaos_index_url, timeout=self.config.request_timeout)
            response.raise_for_status()
            data = response.json()
            self.logger.info(f"Found {len(data)} programs in chaos index")
            return data
        except Exception as e:
            self.logger.error(f"Failed to fetch chaos index: {e}")
            return None
    
    def format_telegram_message(self, program: str, new_subdomains: Set[str], total_count: int) -> str:
        """Format the Telegram notification message"""
        message = f"🚨 <b>New Subdomains Detected!</b>\n\n"
        message += f"📍 <b>Program:</b> {program}\n"
        message += f"🆕 <b>New subdomains:</b> {len(new_subdomains)}\n"
        message += f"📊 <b>Total subdomains:</b> {total_count}\n\n"
        
        # Show up to 20 new subdomains
        subdomain_list = list(new_subdomains)[:20]
        message += "<b>New subdomains:</b>\n"
        for subdomain in subdomain_list:
            message += f"• {subdomain}\n"
        
        if len(new_subdomains) > 20:
            message += f"\n... and {len(new_subdomains) - 20} more\n"
        
        message += f"\n⏰ <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        return message
    
    def rotate_directories(self):
        """Move latest to previous for next run"""
        try:
            if self.config.previous_dir.exists():
                shutil.rmtree(self.config.previous_dir)
                self.logger.info("Removed old previous directory")
            
            if self.config.latest_dir.exists():
                shutil.move(str(self.config.latest_dir), str(self.config.previous_dir))
                self.logger.info("Moved latest to previous directory")
            
            # Recreate latest directory
            self.config.latest_dir.mkdir(exist_ok=True)
            
        except Exception as e:
            self.logger.error(f"Failed to rotate directories: {e}")
    
    def resolve_case_insensitive(self, base_dir: Path, rel_path: Path) -> Optional[Path]:
        """Resolve a path within base_dir by matching each component case-insensitively."""
        current = base_dir
        for part in rel_path.parts:
            if not current.exists():
                return None
            try:
                entries = list(current.iterdir())
            except Exception:
                return None
            mapping = {p.name.lower(): p for p in entries}
            match = mapping.get(part.lower())
            if not match:
                return None
            current = match
        return current if current.exists() else None
    
    def monitor(self):
        """Main monitoring function with new workflow"""
        self.logger.info("=== Starting Chaos Monitor ===")
        
        # Step 1: Fetch chaos index
        chaos_index = self.fetch_chaos_index()
        if not chaos_index:
            self.logger.error("Failed to fetch chaos index")
            return
        
        # Step 2: Download all files concurrently to downloads/latest/
        self.logger.info("Phase 1: Downloading all chaos files...")
        download_results = self.downloader.download_all(chaos_index)
        
        if download_results["successful"] == 0:
            self.logger.error("No files were downloaded successfully")
            return
        
        # Step 3-5: Process each program sequentially to keep memory low
        self.logger.info("Phase 2-4: Extracting, comparing, and notifying program-by-program...")
        new_alerts = 0
        total_new_subdomains = 0
        programs_processed = 0

        latest_zip_files = list(self.config.latest_dir.rglob('*.zip'))
        self.logger.info(f"Found {len(latest_zip_files)} archives in latest")

        # Build a case-insensitive index of previous archives by filename stem to handle moves/reclassification
        prev_index: Dict[str, Path] = {}
        if self.config.previous_dir.exists():
            prev_zip_files = list(self.config.previous_dir.rglob('*.zip'))
            prev_index = {p.stem.lower(): p for p in prev_zip_files}
            self.logger.info(f"Indexed {len(prev_zip_files)} archives in previous")

        for latest_zip in latest_zip_files:
            program_name = latest_zip.stem

            # Determine matching path in previous directory
            try:
                rel_path = latest_zip.relative_to(self.config.latest_dir)
                previous_zip = self.config.previous_dir / rel_path
            except Exception:
                previous_zip = None

            if (previous_zip is None) or (not previous_zip.exists()):
                # Try case-insensitive resolution of path in previous_dir
                if 'rel_path' in locals():
                    ci_match = self.resolve_case_insensitive(self.config.previous_dir, rel_path)
                    if ci_match:
                        self.logger.debug(f"Case-insensitive match found for previous archive: {ci_match}")
                        previous_zip = ci_match

            if (previous_zip is None) or (not previous_zip.exists()):
                # Fallback: look up by filename stem across entire previous tree (case-insensitive)
                lookup = prev_index.get(program_name.lower())
                if lookup:
                    self.logger.debug(f"Stem-based match found for previous archive: {lookup}")
                    previous_zip = lookup

            current_subs = self.extractor.extract_subdomains_from_zip(latest_zip)
            previous_subs = self.extractor.extract_subdomains_from_zip(previous_zip) if previous_zip and previous_zip.exists() else set()

            new_subs = current_subs - previous_subs

            if new_subs:
                total_new_subdomains += len(new_subs)
                self.logger.info(f"Found {len(new_subs)} new subdomains for {program_name}")

                message = self.format_telegram_message(program_name, new_subs, len(current_subs))
                if self.telegram.send_message(message):
                    new_alerts += 1
                    time.sleep(1)

            programs_processed += 1
        
        # Step 6: Rotate directories for next run
        self.logger.info("Phase 5: Rotating directories...")
        self.rotate_directories()
        
        # Summary
        self.logger.info(f"=== Monitoring Complete ===")
        self.logger.info(f"Programs processed: {programs_processed}")
        self.logger.info(f"Total new subdomains found: {total_new_subdomains}")
        self.logger.info(f"Alerts sent: {new_alerts}")
        
        # Always send summary, even if there are no new subdomains
        header = "📊 <b>Chaos Monitor Summary</b>\n\n"
        if new_alerts == 0:
            header = "✅ <b>No new subdomains detected</b>\n\n"
        summary_msg = header
        summary_msg += f"📋 Programs processed: {programs_processed}\n"
        summary_msg += f"🆕 Total new subdomains: {total_new_subdomains}\n"
        summary_msg += f"🔔 Alerts sent: {new_alerts}\n"
        summary_msg += f"⏰ Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        self.telegram.send_message(summary_msg)

def main():
    """Main entry point"""
    config = Config()
    monitor = ChaosMonitor(config)
    
    try:
        monitor.monitor()
    except KeyboardInterrupt:
        logging.info("Monitoring stopped by user")
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)

if __name__ == "__main__":
    main()
