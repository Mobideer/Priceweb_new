import os
import requests
import time
from datetime import datetime
from typing import Optional
try:
    import pytz
except ImportError:
    pytz = None

import config

TIMEZONE = os.environ.get("TZ", "Europe/Moscow")

def get_now_str() -> str:
    """Returns current time string in configured timezone."""
    if pytz:
        try:
            tz = pytz.timezone(TIMEZONE)
            return datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        except:
            pass
    return time.strftime('%Y-%m-%d %H:%M:%S')

def send(text: str, alert_key: Optional[str] = None) -> None:
    """
    Sends a message to Telegram.
    Respects TG_SILENT environment variable.
    """
    # Read settings on demand to ensure they are loaded by config.py
    token = os.environ.get("TG_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TG_CHAT_ID", "").strip()
    silent = os.environ.get("TG_SILENT", "0") == "1"

    if silent:
        return

    if not token or not chat_id:
        print(f"[TG_NOTIFY] Token or Chat ID missing. Token: {'set' if token else 'NOT SET'}, ChatID: {'set' if chat_id else 'NOT SET'}")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    
    try:
        resp = requests.post(url, data=payload, timeout=10)
        if not resp.ok:
            print(f"[TG_NOTIFY] Telegram API Error: {resp.status_code} - {resp.text}")
        else:
            # Successfully sent
            pass
    except Exception as e:
        print(f"[TG_NOTIFY] Failed to send message: {e}")

def notify_start(host: str) -> None:
    send(f"🚀 <b>PriceWeb New Worker</b>\nHost: <code>{host}</code>\nStarted: <code>{get_now_str()}</code>")

def notify_success(stats: dict) -> None:
    msg = (
        "✅ <b>PriceWeb New Worker Success</b>\n"
        f"Items processed: <b>{stats.get('total', 0)}</b>\n"
        f"DB Total Items: <b>{stats.get('items_db', 0)}</b>\n"
        f"DB Size: <b>{stats.get('db_size_mb', 0):.2f} MB</b>\n"
        f"Inserted: <b>{stats.get('inserted', 0)}</b> | "
        f"Changed: <b>{stats.get('changed', 0)}</b>\n"
        f"Snapshots: <b>{stats.get('snapshots_added', 0)}</b>\n"
        f"Time: <b>{stats.get('duration', 0):.2f}s</b>"
    )
    
    new_items = stats.get('new_items', [])
    if new_items:
        msg += "\n\n🆕 <b>New items:</b>\n"
        for name in new_items[:10]:
            msg += f"• {name}\n"
        if len(new_items) > 10:
            msg += "<i>...and more</i>"
            
    send(msg)

def notify_fail(error: str) -> None:
    send(f"❌ <b>PriceWeb New Worker FAILED</b>\nError: <code>{error}</code>")
