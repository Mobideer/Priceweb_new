#!/usr/bin/env python3
import os
import sys
import time
import json
import html
import requests
from typing import Any, Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv

import config

# Load environment variables from .env file FIRST
load_dotenv()

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "").strip()
# RELOAD_TOKEN is read inside trigger_worker to avoid stale global values
API_PORT = config.get_api_port()
PRICE_LOG_PATH = os.environ.get("PRICE_LOG_PATH", "data/cron_log.log")
PRICE_DB_PATH = os.environ.get("PRICE_DB_PATH", "data/priceweb.db")

# Import db module to get status
import db

if not TG_BOT_TOKEN or not TG_CHAT_ID:
    print("Error: TG_BOT_TOKEN or TG_CHAT_ID not set in .env")
    sys.exit(1)

def tg_call(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/{method}"
    try:
        resp = requests.post(url, json=payload, timeout=30)
        return resp.json()
    except Exception as e:
        print(f"TG API Error: {e}")
        return {"ok": False, "error": str(e)}

def tg_send(chat_id: str, text: str, reply_markup: Optional[Dict[str, Any]] = None) -> None:
    payload = {
        "chat_id": chat_id,
        "text": text[:4000],
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    tg_call("sendMessage", payload)

def tg_answer_cb(callback_query_id: str, text: str = "") -> None:
    tg_call("answerCallbackQuery", {
        "callback_query_id": callback_query_id,
        "text": text[:200]
    })

def make_keyboard() -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [{"text": "🚀 Запустить обновление", "callback_data": "run_worker"}],
            [{"text": "📄 Последние логи", "callback_data": "show_log"}],
            [{"text": "📊 Статус БД", "callback_data": "show_status"}],
            [{"text": "🔍 Диагностика", "callback_data": "show_debug"}],
        ]
    }

def format_ts(ts: int) -> str:
    if not ts: return "Никогда"
    try:
        import pytz
        tz = pytz.timezone("Europe/Moscow")
        return datetime.fromtimestamp(ts, tz).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, OSError):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

def get_db_status_text() -> str:
    st = db.get_db_status()
    if not st.get('ok'):
        return f"❌ Ошибка БД: {st.get('error')}"
    
    last_run = format_ts(st.get('worker_last_run_ts', 0))
    
    return (
        "📊 <b>Статус базы данных:</b>\n"
        f"• Файл: <code>{os.path.basename(st.get('db_path', ''))}</code>\n"
        f"• Товаров: <b>{st.get('items_db', 0)}</b>\n"
        f"• Последний запуск: <code>{last_run}</code>"
    )

def get_logs_text(lines: int = 50) -> str:
    if not os.path.exists(PRICE_LOG_PATH):
        return "❌ Файл логов не найден."
    
    try:
        with open(PRICE_LOG_PATH, 'r') as f:
            content = f.readlines()
            tail = "".join(content[-lines:])
            return f"📄 <b>Последние {lines} строк лога:</b>\n<pre>{html.escape(tail)}</pre>"
    except Exception as e:
        return f"❌ Ошибка чтения логов: {e}"

def trigger_worker() -> str:
    # We trigger via the local API to ensure it runs in the same environment/context
    try:
        # Re-load config to ensure we have the latest token
        config.load_config()
        token = os.environ.get("RELOAD_TOKEN", "").strip()
        
        # Use 127.0.0.1 to avoid IPv6 issues with 'localhost' in some environments
        url = f"http://127.0.0.1:{API_PORT}/api/reload"
        headers = {}
        if token:
            headers['Authorization'] = f"Bearer {token}"
            
        # Increased timeout for potentially slow server response
        log_len = len(token) if token else 0
        print(f"[BOT] Triggering worker. API_PORT: {API_PORT}, Token Length: {log_len}")
        
        resp = requests.get(url, headers=headers, timeout=60)
        
        try:
            data = resp.json()
        except Exception:
            return f"❌ <b>Ошибка:</b> Сервер вернул некорректный ответ (Код: {resp.status_code}, Длина: {len(resp.text)})."

        if data.get('ok'):
            return "✅ <b>Воркер запущен!</b>\nРезультат придет в чат после завершения."
        else:
            return f"❌ <b>Ошибка запуска:</b> {data.get('error')}\n(API Port: {API_PORT}, Sent Token Len: {len(params.get('token',''))})"
    except requests.exceptions.Timeout:
        return f"❌ <b>Таймаут:</b> Сервер не ответил вовремя (30с)."
    except Exception as e:
        return f"❌ <b>Не удалось связаться с API:</b> {e}\n(Убедитесь, что сервер запущен на порту {API_PORT})"

def get_debug_text() -> str:
    config.load_config()
    token = os.environ.get("RELOAD_TOKEN", "")
    db_path = os.environ.get("PRICE_DB_PATH", "data/priceweb.db")
    log_path = config.get_log_path()
    return (
        "🔍 <b>Бот Диагностика:</b>\n"
        f"• API Port: <code>{API_PORT}</code>\n"
        f"• RELOAD_TOKEN: <code>{'SET (len=' + str(len(token)) + ')' if token else 'MISSING'}</code>\n"
        f"• DB Path: <code>{db_path}</code>\n"
        f"• Log Path: <code>{log_path}</code>\n"
        f"• Working Dir: <code>{os.getcwd()}</code>\n"
        f"• Script: <code>{os.path.abspath(__file__)}</code>"
    )

def handle_callback(cb: Dict[str, Any]) -> None:
    chat_id = str(cb.get("message", {}).get("chat", {}).get("id", ""))
    if chat_id != TG_CHAT_ID: return
    
    data = cb.get("data")
    tg_answer_cb(cb.get("id"), "Выполняю...")
    
    if data == "run_worker":
        tg_send(chat_id, trigger_worker(), reply_markup=make_keyboard())
    elif data == "show_log":
        tg_send(chat_id, get_logs_text(), reply_markup=make_keyboard())
    elif data == "show_status":
        tg_send(chat_id, get_db_status_text(), reply_markup=make_keyboard())
    elif data == "show_debug":
        tg_send(chat_id, get_debug_text(), reply_markup=make_keyboard())
    elif data == "delete_missing":
        missing_file = "data/missing_items.json"
        if not os.path.exists(missing_file):
            tg_send(chat_id, "❌ Файл со списком удаления не найден.", reply_markup=make_keyboard())
            return
            
        try:
            with open(missing_file, 'r', encoding='utf-8') as f:
                items = json.load(f)
            
            if not items:
                tg_send(chat_id, "⚠️ Список пуст.", reply_markup=make_keyboard())
                return
                
            conn = db.get_connection()
            count = 0
            for item in items:
                sku = item['sku']
                conn.execute("DELETE FROM items_latest WHERE sku = ?", (sku,))
                conn.execute("DELETE FROM item_snapshots WHERE sku = ?", (sku,))
                count += 1
            conn.commit()
            conn.close()
            
            os.remove(missing_file)
            tg_send(chat_id, f"✅ Удалено <b>{count}</b> товаров из базы данных.", reply_markup=make_keyboard())
            
        except Exception as e:
            tg_send(chat_id, f"❌ Ошибка удаления: {e}", reply_markup=make_keyboard())
            
    elif data == "ignore_missing":
        missing_file = "data/missing_items.json"
        if os.path.exists(missing_file):
            os.remove(missing_file)
        tg_send(chat_id, "👌 Список пропущен.", reply_markup=make_keyboard())

def main():
    print("🤖 Бот запущен...")
    offset = 0
    # Welcome message
    tg_send(TG_CHAT_ID, "🤖 <b>PriceWeb New Bot</b> активен.\nИспользуйте меню для управления:", reply_markup=make_keyboard())
    
    while True:
        try:
            resp = tg_call("getUpdates", {
                "offset": offset,
                "timeout": 30,
                "allowed_updates": ["message", "callback_query"]
            })
            
            if not resp.get("ok"):
                time.sleep(5)
                continue
                
            for upd in resp.get("result", []):
                offset = upd.get("update_id") + 1
                
                if "callback_query" in upd:
                    handle_callback(upd["callback_query"])
                elif "message" in upd:
                    msg = upd["message"]
                    if msg.get("text") in ["/start", "/menu"]:
                        status_msg = "Меню управления:"
                        # Check token dynamically
                        token = os.environ.get("RELOAD_TOKEN", "")
                        if not token:
                            status_msg = "⚠️ <b>Внимание:</b> RELOAD_TOKEN не установлен. Бот не сможет запускать обновление.\n\n" + status_msg
                        tg_send(TG_CHAT_ID, status_msg, reply_markup=make_keyboard())
                        
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
