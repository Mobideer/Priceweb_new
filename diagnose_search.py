import db
import sqlite3
import os

def diagnose():
    db.ensure_schema()
    conn = db.get_connection()
    try:
        print("Checking tables...")
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_names = [t[0] for t in tables]
        print(f"Tables found: {table_names}")
        
        if 'items_search' not in table_names:
            print("❌ items_search table is missing!")
        else:
            count = conn.execute("SELECT count(*) FROM items_search").fetchone()[0]
            main_count = conn.execute("SELECT count(*) FROM items_latest").fetchone()[0]
            print(f"✅ items_search has {count} rows (main table has {main_count})")
            
            if count == 0 and main_count > 0:
                print("🔄 Index is empty. Repairing...")
                conn.execute("INSERT INTO items_search(rowid, sku, name) SELECT rowid, sku, name FROM items_latest")
                conn.commit()
                print(f"✅ Repopulated. New count: {conn.execute('SELECT count(*) FROM items_search').fetchone()[0]}")

        print("\nChecking triggers...")
        triggers = conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'").fetchall()
        print(f"Triggers found: {[t[0] for t in triggers]}")

        print("\nTesting Search Logic...")
        # Test basic tokens
        test_q = "GG CF226"
        tokens = [f"{t}*" for t in test_q.split()]
        fts_query = " AND ".join(tokens)
        print(f"Query: {fts_query}")
        
        res = conn.execute("""
            SELECT i.sku, i.name FROM items_latest i
            JOIN items_search s ON i.rowid = s.rowid
            WHERE s.items_search MATCH ?
            LIMIT 5
        """, (fts_query,)).fetchall()
        
        if res:
            print(f"✅ Found {len(res)} results for '{test_q}':")
            for r in res:
                print(f"  - {r[0]}: {r[1]}")
        else:
            print(f"❌ No results for '{test_q}' via FTS5")

    except Exception as e:
        print(f"❌ Error during diagnosis: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    diagnose()
