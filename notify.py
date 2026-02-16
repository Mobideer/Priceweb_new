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

def send(text: str, alert_key: Optional[str] = None, reply_markup: Optional[dict] = None) -> None:
    """
    Sends a message to Telegram.
    Respects TG_SILENT environment variable.
    """
    # Read settings on demand to ensure they are loaded by config.py
    token = os.environ.get("TG_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TG_CHAT_ID", "").strip()
    silent = os.environ.get("TG_SILENT", "0") == "1"

    # Write to log file if possible
    log_path = config.get_log_path()

    def _log_tg(msg):
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [TG] {msg}\n")

    if silent:
        _log_tg("Silent mode is ON, skipping.")
        return

    if not token or not chat_id:
        error_msg = f"Token or Chat ID missing. Token: {'set' if token else 'NOT SET'}, ChatID: {'set' if chat_id else 'NOT SET'}"
        print(f"[TG_NOTIFY] {error_msg}")
        _log_tg(error_msg)
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if not resp.ok:
            err = f"Telegram API Error: {resp.status_code} - {resp.text}"
            print(f"[TG_NOTIFY] {err}")
            _log_tg(err)
        else:
            _log_tg("Successfully sent message to Telegram.")
    except Exception as e:
        err = f"Failed to send message: {e}"
        print(f"[TG_NOTIFY] {err}")
        _log_tg(err)

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

def notify_price_changes(changes: list) -> None:
    """
    Sends a report of sharp price changes to Telegram.
    changes: List of dicts with keys: name, sku, old_price, new_price, diff_pct
    """
    if not changes:
        return

    msg = f"⚠️ <b>Резкое изменение цен ({len(changes)} шт):</b>\n\n"
    
    # Send in chunks if too long, but for now just clamp to first 20 items to avoid hitting limits
    MAX_ITEMS = 20
    
    for item in changes[:MAX_ITEMS]:
        emoji = "📈" if item['new_price'] > item['old_price'] else "📉"
        msg += (
            f"{emoji} <b>{item['name']}</b> ({item['sku']})\n"
            f"   {item['old_price']} ➡️ <b>{item['new_price']}</b> ({item['diff_pct']:+.1f}%)\n"
        )
        
    if len(changes) > MAX_ITEMS:
        msg += f"\n<i>...и еще {len(changes) - MAX_ITEMS} товаров.</i>"
        
    send(msg)

def notify_missing_items(missing_items: list) -> None:
    """
    Sends a report about items missing from the feed (likely deleted).
    """
    if not missing_items:
        return

    msg = f"🗑️ <b>Обнаружено {len(missing_items)} удаленных товаров:</b>\n"
    
    for item in missing_items[:10]:
        msg += f"• {item['name']} ({item['sku']})\n"
        
    if len(missing_items) > 10:
        msg += f"<i>...и еще {len(missing_items) - 10} шт.</i>\n"
        
    msg += "\n<b>Удалить их из базы данных?</b>"
    
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "❌ Удалить из БД", "callback_data": "delete_missing"},
                {"text": "Пропустить", "callback_data": "ignore_missing"}
            ]
        ]
    }
    
    send(msg, reply_markup=keyboard)
