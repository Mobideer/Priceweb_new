import db
import sqlite3
import os
import notify
import config
import requests
import json

def diagnose():
    config.load_config()
    db.ensure_schema()
    conn = db.get_connection()
    try:
        print("--- 📦 DATABASE CONTENT CHECK ---")
        target_sku = "gg-cf226xl"
        print(f"Checking for SKU: '{target_sku}'")
        
        row_main = conn.execute("SELECT rowid, sku, name FROM items_latest WHERE lower(sku) = ?", (target_sku.lower(),)).fetchone()
        if row_main:
            print(f"✅ Found in items_latest: RowID={row_main[0]}, SKU='{row_main[1]}', Name='{row_main[2][:30]}...'")
            
            row_fts = conn.execute("SELECT rowid, sku, name FROM items_search WHERE rowid = ?", (row_main[0],)).fetchone()
            if row_fts:
                print(f"✅ Found in items_search by RowID: SKU='{row_fts[1]}', Name='{row_fts[2][:30]}...'")
            else:
                print("❌ NOT found in items_search by RowID!")
        else:
            print(f"❌ '{target_sku}' NOT FOUND in items_latest. Checking with LIKE...")
            like_res = conn.execute("SELECT sku FROM items_latest WHERE sku LIKE '%cf226%' LIMIT 5").fetchall()
            print(f"Similar SKUs in items_latest: {[r[0] for r in like_res]}")

        print("\n--- 🔍 SEARCH SYNTAX LAB ---")
        test_queries = [
            "gg cf226",
            "gg* AND cf226*",
            "\"gg\" AND \"cf226\"",
            "\"gg-cf226xl\"",
            "cf226"
        ]
        
        for q in test_queries:
            try:
                res = conn.execute("SELECT sku FROM items_search WHERE items_search MATCH ? LIMIT 1", (q,)).fetchone()
                status = "✅ MATCHED" if res else "❌ NO MATCH"
                print(f"Match '{q}': {status}")
            except Exception as e:
                print(f"Match '{q}': ❌ ERROR ({e})")

        print("\n--- 📱 TELEGRAM DEEP CHECK ---")
        token = os.environ.get("TG_BOT_TOKEN", "").strip()
        chat_id = os.environ.get("TG_CHAT_ID", "").strip()
        
        if not token or not chat_id:
            print("❌ TG_BOT_TOKEN or TG_CHAT_ID is missing!")
        else:
            print(f"Attempting to send message to ChatID {chat_id}...")
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": "🧪 <b>Deep Diagnostic Test</b>\nIf you see this, the bot and chat ID are definitely correct.",
                "parse_mode": "HTML"
            }
            try:
                resp = requests.post(url, data=payload, timeout=10)
                print(f"Telegram API Response Status: {resp.status_code}")
                print(f"Telegram API Response Body: {resp.text}")
                data = resp.json()
                if data.get('ok'):
                    print("✅ Telegram says OK! If you don't see the message, verify you're looking at the right bot/chat.")
                else:
                    print(f"❌ Telegram says NOT OK: {data.get('description')}")
            except Exception as e:
                print(f"❌ Network error calling Telegram: {e}")

    except Exception as e:
        print(f"❌ DIAGNOSIS ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    diagnose()
