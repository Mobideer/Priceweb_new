import os
import requests
import time
from typing import Optional

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "").strip()
TG_SILENT = os.environ.get("TG_SILENT", "0") == "1"

def send(text: str, alert_key: Optional[str] = None) -> None:
    """
    Sends a message to Telegram.
    Respects TG_SILENT environment variable.
    """
    if TG_SILENT:
        return

    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print(f"[TG_NOTIFY] Token or Chat ID missing. Message: {text}")
        return

    # In a full production version, we would verify cooldown logic with DB here
    # For now, we assume simple direct sending or basic cooldown in worker
    
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        print(f"[TG_NOTIFY] Failed to send message: {e}")

def notify_start(host: str) -> None:
    send(f"🚀 <b>PriceWeb New Worker</b>\nHost: <code>{host}</code>\nStarted: <code>{time.strftime('%Y-%m-%d %H:%M:%S')}</code>")

def notify_success(stats: dict) -> None:
    msg = (
        "✅ <b>PriceWeb New Worker Success</b>\n"
        f"Items: <b>{stats.get('total', 0)}</b>\n"
        f"Inserted: <b>{stats.get('inserted', 0)}</b>\n"
        f"Changed: <b>{stats.get('changed', 0)}</b>\n"
        f"Snapshots: <b>{stats.get('snapshots_added', 0)}</b>\n"
        f"Time: <b>{stats.get('duration', 0):.2f}s</b>"
    )
    send(msg)

def notify_fail(error: str) -> None:
    send(f"❌ <b>PriceWeb New Worker FAILED</b>\nError: <code>{error}</code>")
