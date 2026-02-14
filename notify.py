import os
import requests
import time
from datetime import datetime
from typing import Optional
try:
    import pytz
except ImportError:
    pytz = None

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "").strip()
TG_SILENT = os.environ.get("TG_SILENT", "0") == "1"
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
