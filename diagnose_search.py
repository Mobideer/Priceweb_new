import db
import sqlite3
import os
import notify
import config

def diagnose():
    db.ensure_schema()
    conn = db.get_connection()
    try:
        print("--- 📦 DATABASE CHECK ---")
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_names = [t[0] for t in tables]
        print(f"Tables found: {table_names}")
        
        main_count = conn.execute("SELECT count(*) FROM items_latest").fetchone()[0]
        print(f"Main table (items_latest) count: {main_count}")

        if 'items_search' in table_names:
            fts_count = conn.execute("SELECT count(*) FROM items_search").fetchone()[0]
            print(f"FTS table (items_search) count: {fts_count}")
            
            # Check rowid alignment
            print("\nChecking rowid alignment...")
            mismatch = conn.execute("""
                SELECT count(*) FROM items_latest i
                LEFT JOIN items_search s ON i.rowid = s.rowid
                WHERE s.rowid IS NULL
            """).fetchone()[0]
            if mismatch > 0:
                print(f"❌ {mismatch} items are in main table but MISSING from search index (rowid mismatch)!")
                print("🔄 Repairing index (Full rebuild)...")
                conn.execute("DELETE FROM items_search")
                conn.execute("INSERT INTO items_search(rowid, sku, name) SELECT rowid, sku, name FROM items_latest")
                conn.commit()
                print("✅ Index rebuilt.")
            else:
                print("✅ Search index rowids are perfectly aligned with main table.")

        print("\n--- 🔍 SEARCH TEST ---")
        test_q = "GG CF226"
        tokens = [f"{t}*" for t in test_q.split()]
        fts_query = " AND ".join(tokens)
        print(f"Testing FTS query: '{fts_query}'")
        
        # Test 1: Direct FTS search (no join)
        res_fts = conn.execute("SELECT rowid, sku, name FROM items_search WHERE items_search MATCH ? LIMIT 3", (fts_query,)).fetchall()
        if res_fts:
            print(f"✅ FTS table matched {len(res_fts)} items directly:")
            for r in res_fts:
                print(f"  - RowID: {r[0]}, SKU: {r[1]}, Name: {r[2][:30]}...")
        else:
            print(f"❌ FTS table matched NOTHING directly for '{fts_query}'")
            # Let's see what IS in there
            sample = conn.execute("SELECT sku, name FROM items_search LIMIT 3").fetchall()
            print("Sample data in items_search:")
            for s in sample:
                print(f"  - {s[0]}: {s[1][:30]}...")

        # Test 2: Search with JOIN (as used in app.py)
        res_full = conn.execute("""
            SELECT i.sku, i.name FROM items_latest i
            JOIN items_search s ON i.rowid = s.rowid
            WHERE s.items_search MATCH ?
            LIMIT 3
        """, (fts_query,)).fetchall()
        if res_full:
            print("✅ JOIN search works!")
        else:
            print("❌ JOIN search failed.")

        print("\n--- 📱 TELEGRAM NOTIFICATION CHECK ---")
        # Ensure config is loaded (though db.py usually does it)
        config.load_config()
        
        token = os.environ.get("TG_BOT_TOKEN", "")
        chat_id = os.environ.get("TG_CHAT_ID", "")
        
        print(f"Env file loaded: {'Yes' if os.path.exists('.env') else 'No (.env not found in current dir: ' + os.getcwd() + ')'}")
        print(f"System env file: {'Yes' if os.path.exists('/etc/priceweb_new.env') else 'No'}")
        print(f"TG_BOT_TOKEN: {'SET (ends with ' + token[-4:] + ')' if token else 'NOT SET'}")
        print(f"TG_CHAT_ID: {'SET (' + chat_id + ')' if chat_id else 'NOT SET'}")
        
        print("\nSending test notification via notify.send()...")
        try:
            notify.send("⚡️ <b>Server Diagnostic Test</b>\nIf you see this, notifications are working!")
            print("✅ notify.send() call completed. Check your Telegram!")
        except Exception as e:
            print(f"❌ Error sending notification: {e}")

    except Exception as e:
        print(f"❌ DIAGNOSIS ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    diagnose()
