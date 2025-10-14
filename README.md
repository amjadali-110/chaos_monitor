# Chaos Monitor Bot

Monitors ProjectDiscovery Chaos data and sends Telegram alerts when new subdomains are added.

## Setup (GitHub Actions)

1. Add repository secrets:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
2. Commit this repo.
3. Action runs hourly, uploads logs and downloads.

For local testing:
```bash
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_CHAT_ID=...
python chaos_monitor.py
```
