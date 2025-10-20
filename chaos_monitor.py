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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import subprocess
import platform
import tarfile

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
        self.outputs_dir = Path(os.getenv('OUTPUTS_DIR', 'outputs'))
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
            allowed_methods=frozenset(["GET", "HEAD"]),
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
        
        # Reset results for this run
        self.results = []
        
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
    
    def ensure_pd_tools(self) -> None:
        """Ensure Go and ProjectDiscovery tools are installed, but only when scans are needed."""
        try:
            go_path = shutil.which("go")
            if not go_path:
                # Attempt local, non-root Go install
                self.logger.info("Go not found; attempting local installation")
                try:
                    # Determine platform
                    sys = platform.system().lower()
                    arch = platform.machine().lower()
                    if sys.startswith("linux"):
                        os_tag = "linux"
                    elif sys.startswith("darwin"):
                        os_tag = "darwin"
                    else:
                        os_tag = sys
                    if arch in ("x86_64", "amd64"):
                        arch_tag = "amd64"
                    elif arch in ("aarch64", "arm64"):
                        arch_tag = "arm64"
                    else:
                        arch_tag = arch

                    # Get latest Go version
                    version = "go1.22.5"
                    try:
                        r = requests.get("https://go.dev/VERSION?m=text", timeout=20)
                        if r.ok and r.text.strip().startswith("go"):
                            version = r.text.strip().splitlines()[0]
                    except Exception:
                        pass

                    url = f"https://dl.google.com/go/{version}.{os_tag}-{arch_tag}.tar.gz"
                    tools_dir = Path(".tools")
                    tools_dir.mkdir(parents=True, exist_ok=True)
                    tar_path = tools_dir / "go.tgz"

                    with requests.get(url, stream=True, timeout=60) as resp:
                        resp.raise_for_status()
                        with open(tar_path, "wb") as f:
                            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                                if chunk:
                                    f.write(chunk)
                    # Extract
                    goroot = tools_dir / "go"
                    if goroot.exists():
                        shutil.rmtree(goroot, ignore_errors=True)
                    with tarfile.open(tar_path, "r:gz") as tf:
                        # The archive root is "go/"; extract into tools_dir
                        tf.extractall(path=tools_dir)
                    # Update environment
                    os.environ["GOROOT"] = str(goroot)
                    os.environ["PATH"] = str(goroot / "bin") + os.pathsep + os.environ.get("PATH", "")
                    go_path = str(goroot / "bin" / "go")
                    self.logger.info(f"Installed Go at {goroot}")
                except Exception as e:
                    self.logger.warning(f"Failed to auto-install Go: {e}")
                    return

            # Ensure GOPATH/bin is on PATH for this process
            try:
                gopath = subprocess.check_output([go_path, "env", "GOPATH"], text=True).strip()
                if not gopath:
                    gopath = str(Path.home() / "go")
                bin_dir = os.path.join(gopath, "bin")
                if bin_dir and bin_dir not in os.environ.get("PATH", ""):
                    os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
            except Exception as e:
                self.logger.debug(f"Failed to resolve GOPATH: {e}")

            tools = {
                "dnsx": "github.com/projectdiscovery/dnsx/cmd/dnsx@latest",
                "httpx": "github.com/projectdiscovery/httpx/cmd/httpx@latest",
                "nuclei": "github.com/projectdiscovery/nuclei/v3/cmd/nuclei@latest",
            }
            for tool, module in tools.items():
                if shutil.which(tool):
                    continue
                self.logger.info(f"Installing missing tool: {tool}")
                subprocess.run([go_path, "install", "-v", module], check=False)
                if not shutil.which(tool):
                    self.logger.warning(f"{tool} still not found after install attempt")

            # Optionally update nuclei templates if nuclei is available
            if shutil.which("nuclei"):
                subprocess.run(["nuclei", "-ut"], check=False)
        except Exception as e:
            self.logger.warning(f"Tool installation step failed: {e}")

    def run_scans(self, run_outputs_dir: Path, all_new_path: Path) -> None:
        """Run dnsx, httpx, and nuclei scans on the aggregated new subdomains file."""
        try:
            # Ensure tools present (for local runs; CI also installs them)
            self.ensure_pd_tools()

            scans_dir = run_outputs_dir / "Scans"
            scans_dir.mkdir(parents=True, exist_ok=True)

            # dnsx step
            if shutil.which("dnsx"):
                dnsx_first = scans_dir / "dnsx_first.txt"
                final_dnsx = scans_dir / "final-dnsx.txt"
                self.logger.info(f"Running dnsx on {all_new_path.name}")
                self.telegram.send_message("dnsx scan run")
                dnsx_cmd = [
                    "dnsx", "-l", str(all_new_path),
                    "-cname", "-aaaa", "-a", "-mx", "-ns",
                    "-retry", "5", "-o", str(dnsx_first)
                ]
                dnsx_proc = subprocess.run(dnsx_cmd, check=False)
                # Post-process: sort -u into final-dnsx.txt
                try:
                    with open(dnsx_first, 'r', encoding='utf-8', errors='ignore') as rf:
                        lines = [ln.strip() for ln in rf if ln.strip()]
                    uniq = sorted(set(lines))
                    with open(final_dnsx, 'w', encoding='utf-8') as wf:
                        if uniq:
                            wf.write("\n".join(uniq) + "\n")
                        else:
                            wf.write("")
                except Exception as e:
                    self.logger.warning(f"Failed to post-process dnsx output: {e}")
                finally:
                    if dnsx_proc.returncode == 0:
                        self.telegram.send_message("dnsx scan done")
                    else:
                        self.telegram.send_message("dnsx scan failed")
            else:
                self.logger.warning("dnsx not found; skipping dnsx step")
                self.telegram.send_message("dnsx scan skipped (not found)")

            # httpx step
            final_dnsx = scans_dir / "final-dnsx.txt"
            if final_dnsx.exists() and shutil.which("httpx"):
                final_httpx = scans_dir / "final-httpx.txt"
                self.logger.info("Running httpx on dnsx results")
                self.telegram.send_message("httpx scan run")
                httpx_cmd = ["httpx", "-l", str(final_dnsx), "-o", str(final_httpx)]
                httpx_proc = subprocess.run(httpx_cmd, check=False)
                if httpx_proc.returncode == 0:
                    self.telegram.send_message("httpx scan done")
                else:
                    self.telegram.send_message("httpx scan failed")
            else:
                if not shutil.which("httpx"):
                    self.logger.warning("httpx not found; skipping httpx step")
                    self.telegram.send_message("httpx scan skipped (not found)")
                elif not final_dnsx.exists():
                    self.telegram.send_message("httpx scan skipped (no input)")

            # nuclei step
            final_httpx = scans_dir / "final-httpx.txt"
            if final_httpx.exists() and shutil.which("nuclei"):
                nuclei_dir = scans_dir / "Nuclei"
                nuclei_dir.mkdir(parents=True, exist_ok=True)
                severities = ["high", "medium", "critical", "low", "info", "unknown"]
                for sev in severities:
                    out_file = nuclei_dir / f"nuclei_{sev}.txt"
                    self.logger.info(f"Running nuclei severity={sev}")
                    self.telegram.send_message(f"nuclei {sev} scan run")
                    nuclei_cmd = ["nuclei", "-l", str(final_httpx), "-severity", sev, "-o", str(out_file)]
                    nuclei_proc = subprocess.run(nuclei_cmd, check=False)
                    if nuclei_proc.returncode == 0:
                        self.telegram.send_message(f"nuclei {sev} scan done")
                    else:
                        self.telegram.send_message(f"nuclei {sev} scan failed")
            else:
                if not shutil.which("nuclei"):
                    self.logger.warning("nuclei not found; skipping nuclei step")
                    self.telegram.send_message("nuclei scans skipped (not found)")
                elif not final_httpx.exists():
                    self.telegram.send_message("nuclei scans skipped (no input)")
        except Exception as e:
            self.logger.error(f"Scan pipeline failed: {e}")
            self.telegram.send_message("scan pipeline failed")

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

        # Lazy-create outputs directory only when new subdomains are found
        run_outputs_dir: Optional[Path] = None
        all_new_subs: Set[str] = set()
        added_files: List[tuple[str, int]] = []

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

                # Determine platform and subfolder from latest zip relative path
                try:
                    rel_path = latest_zip.relative_to(self.config.latest_dir)
                    platform = rel_path.parts[0]
                    subfolder = rel_path.parts[1] if len(rel_path.parts) > 1 else "Bounty"
                except Exception:
                    platform = "self-hosted"
                    subfolder = "Bounty"

                # Write per-program new subdomains file under outputs/platform/subfolder/
                try:
                    # Lazily create run_outputs_dir on first detection
                    if run_outputs_dir is None:
                        try:
                            self.config.outputs_dir.mkdir(parents=True, exist_ok=True)
                            timestamp = datetime.now().strftime('%d-%m-%y-%H-%M')
                            run_outputs_dir = self.config.outputs_dir / timestamp
                            run_outputs_dir.mkdir(parents=True, exist_ok=True)
                        except Exception as e:
                            self.logger.error(f"Failed to prepare outputs directory: {e}")
                            run_outputs_dir = self.config.outputs_dir
                    out_dir = run_outputs_dir / platform / subfolder
                    out_dir.mkdir(parents=True, exist_ok=True)
                    program_file = out_dir / f"{program_name.lower()}.txt"
                    with open(program_file, 'w', encoding='utf-8') as pf:
                        for s in sorted(new_subs):
                            pf.write(s + "\n")
                except Exception as e:
                    self.logger.error(f"Failed to write outputs for {program_name}: {e}")

                # Track this file and count for aggregated Telegram update
                try:
                    rel_file_path = (run_outputs_dir / platform / subfolder / f"{program_name.lower()}.txt").as_posix()
                    added_files.append((rel_file_path, len(new_subs)))
                except Exception:
                    pass

                # Accumulate all new subdomains for global file
                all_new_subs.update(new_subs)

            programs_processed += 1
        
        # After processing all, write all-new-subs.txt only if there were new subdomains
        if run_outputs_dir is not None and all_new_subs:
            all_new_path = run_outputs_dir / 'all-new-subs.txt'
            try:
                with open(all_new_path, 'w', encoding='utf-8') as f:
                    for s in sorted(all_new_subs):
                        f.write(s + "\n")
            except Exception as e:
                self.logger.error(f"Failed to write all-new-subs.txt: {e}")
            else:
                # Run dnsx, httpx and nuclei scans
                self.run_scans(run_outputs_dir, all_new_path)
        
        # Step 6: Rotate directories for next run
        self.logger.info("Phase 5: Rotating directories...")
        self.rotate_directories()
        
        # Summary
        self.logger.info(f"=== Monitoring Complete ===")
        self.logger.info(f"Programs processed: {programs_processed}")
        self.logger.info(f"Total new subdomains found: {total_new_subdomains}")
        self.logger.info(f"Files updated: {len(added_files)}")
        
        # Telegram: minimal summary only
        if added_files:
            msg = (
                f"✅ Scan complete\n"
                f"📊 New subdomains: {total_new_subdomains}\n"
                f"🗂️ Files updated: {len(added_files)}\n"
                f"📋 Programs processed: {programs_processed}\n"
                f"📦 Output: {run_outputs_dir.as_posix()}\n"
                f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        else:
            msg = "✅ No new subdomains detected"
        self.telegram.send_message(msg)

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
