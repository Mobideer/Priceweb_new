import db
import sqlite3
import os
import notify
import config
import requests
import json
import time

def diagnose():
    config.load_config()
    db.ensure_schema() # This will create triggers if missing
    conn = db.get_connection()
    try:
        print("--- 📦 DATABASE REPAIR & SYNC ---")
        
        # Force standard FTS5 schema (without 'content' mapping which failed on server)
        print("Fixing Search Index schema...")
        conn.execute("DROP TABLE IF EXISTS items_search")
        conn.execute("CREATE VIRTUAL TABLE items_search USING fts5(sku, name)")
        
        main_count = conn.execute("SELECT count(*) FROM items_latest").fetchone()[0]
        print(f"Main table count: {main_count}")
        
        print("Repopulating index (Full rebuild)...")
        conn.execute("INSERT INTO items_search(rowid, sku, name) SELECT rowid, sku, name FROM items_latest")
        conn.commit()
        
        fts_count = conn.execute("SELECT count(*) FROM items_search").fetchone()[0]
        print(f"FTS index rebuilt. New count: {fts_count}")

        print("\n--- 🔍 SEARCH FINAL TEST ---")
        # Standardize query for FTS5 on this environment
        # We use a simpler query first
        test_q = "gg cf226"
        # Wrap tokens in quotes to handle hyphens and special chars
        fts_query = ' '.join([f'"{t}"' for t in test_q.split()])
        print(f"Testing FTS query: '{fts_query}'")
        
        res = conn.execute("""
            SELECT i.sku, i.name FROM items_latest i
            JOIN items_search s ON i.rowid = s.rowid
            WHERE s.items_search MATCH ?
            LIMIT 5
        """, (fts_query,)).fetchall()
        
        if res:
            print(f"✅ SUCCESS! Found {len(res)} results for '{test_q}':")
            for r in res:
                print(f"  - {r[0]}: {r[1][:30]}...")
        else:
            print(f"❌ Still no results for '{fts_query}'. Trying fallback 'LIKE'...")
            fallback = conn.execute("SELECT sku FROM items_latest WHERE sku LIKE '%cf226%' LIMIT 1").fetchone()
            if fallback:
                print(f"  (Note: '{fallback[0]}' exists in main table, but FTS5 didn't find it)")

        print("\n--- 📱 TELEGRAM HEARTBEAT ---")
        token = os.environ.get("TG_BOT_TOKEN", "").strip()
        chat_id = os.environ.get("TG_CHAT_ID", "").strip()
        silent = os.environ.get("TG_SILENT", "0")
        
        now = time.strftime('%H:%M:%S')
        print(f"Current server time: {now}")
        print(f"TG_SILENT: {silent} ({'SILENT MODE IS ON - No notifications will be sent!' if silent == '1' else 'Off'})")
        
        msg = f"🔔 <b>HEARTBEAT [{now}]</b>\nIf you see this, notifications are WORKING on the server."
        print(f"Sending heartbeat to ChatID {chat_id}...")
        
        try:
            resp = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                                 data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, 
                                 timeout=10)
            if resp.ok:
                print("✅ Telegram OK. Please check your phone RIGHT NOW.")
            else:
                print(f"❌ Telegram Error: {resp.text}")
        except Exception as e:
            print(f"❌ Connection Error: {e}")

    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    diagnose()
