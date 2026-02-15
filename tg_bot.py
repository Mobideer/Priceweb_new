#!/usr/bin/env python3
import os
import sys
import time
import json
import html
import requests
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv

import config

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "").strip()
RELOAD_TOKEN = os.environ.get("RELOAD_TOKEN", "").strip()
API_PORT = config.get_api_port()

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "").strip()
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
        ]
    }

def get_db_status_text() -> str:
    st = db.get_db_status()
    if not st.get('ok'):
        return f"❌ Ошибка БД: {st.get('error')}"
    
    last_run = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(st.get('worker_last_run_ts', 0))) if st.get('worker_last_run_ts') else "Никогда"
    
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
        url = f"http://localhost:{API_PORT}/api/reload"
        params = {}
        if RELOAD_TOKEN:
            params['token'] = RELOAD_TOKEN
            
        resp = requests.get(url, params=params, timeout=10)
        
        if resp.status_code == 403:
            return "❌ <b>Ошибка:</b> Доступ запрещен (проверьте RELOAD_TOKEN)."
            
        data = resp.json()
        if data.get('ok'):
            return "✅ <b>Воркер запущен!</b>\nРезультат придет в чат после завершения."
        else:
            return f"❌ <b>Ошибка запуска:</b> {data.get('error')}"
    except Exception as e:
        return f"❌ <b>Не удалось связаться с API:</b> {e}\n(Убедитесь, что сервер запущен на порту {API_PORT})"

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
                        tg_send(TG_CHAT_ID, "Меню управления:", reply_markup=make_keyboard())
                        
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
