import os
import sqlite3
import time
import json
from typing import Dict, Any, List, Optional, Tuple

DB_PATH = os.environ.get("PRICE_DB_PATH", "data/priceweb.db")

def get_connection(timeout: int = 30) -> sqlite3.Connection:
    # Ensure directory exists if path is deeper than root
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
        
    conn = sqlite3.connect(DB_PATH, timeout=timeout)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_schema() -> None:
    conn = get_connection()
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        
        # items_latest: Current state of items
        conn.execute("""
            CREATE TABLE IF NOT EXISTS items_latest (
                sku TEXT PRIMARY KEY,
                name TEXT,
                our_price REAL,
                our_qty REAL,
                
                my_sklad_price REAL,
                my_sklad_qty REAL,
                
                min_sup_price REAL,
                min_sup_qty REAL,
                min_sup_supplier TEXT,
                
                suppliers_json TEXT,
                updated_at INTEGER,
                created_at INTEGER
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_items_name ON items_latest(name);")

        # Migration: Add created_at if missing
        try:
            conn.execute("ALTER TABLE items_latest ADD COLUMN created_at INTEGER;")
            # Initialize existing items with updated_at as fallback
            conn.execute("UPDATE items_latest SET created_at = updated_at WHERE created_at IS NULL;")
        except sqlite3.OperationalError:
            # Column already exists
            pass
        
        conn.execute("CREATE INDEX IF NOT EXISTS idx_items_created_at ON items_latest(created_at);")
        
        # item_snapshots: Daily history
        conn.execute("""
            CREATE TABLE IF NOT EXISTS item_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT NOT NULL,
                ts INTEGER NOT NULL,
                
                our_price REAL,
                our_qty REAL,
                
                my_sklad_price REAL,
                my_sklad_qty REAL,
                
                min_sup_price REAL,
                min_sup_qty REAL,
                min_sup_supplier TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_sku_ts ON item_snapshots(sku, ts);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_ts ON item_snapshots(ts);")

        # meta: Key-value storage for service info
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                k TEXT PRIMARY KEY,
                v TEXT
            )
        """)
        conn.commit()
    finally:
        conn.close()

def load_existing_latest(conn: sqlite3.Connection) -> Dict[str, Tuple]:
    cur = conn.execute("""
        SELECT sku, name, suppliers_json, 
               our_price, our_qty, 
               my_sklad_price, my_sklad_qty, 
               min_sup_price, min_sup_qty, min_sup_supplier,
               created_at
        FROM items_latest
    """)
    out = {}
    for row in cur.fetchall():
        out[row[0]] = row[1:]
    return out

def get_db_status() -> Dict[str, Any]:
    if not os.path.exists(DB_PATH):
        return {"ok": False, "error": "DB not found"}
    
    conn = get_connection()
    try:
        worker_ts = 0
        r = conn.execute("SELECT v FROM meta WHERE k='last_reload_ts'").fetchone()
        if r and r[0]:
            worker_ts = int(r[0])
            
        items_count = conn.execute("SELECT COUNT(*) FROM items_latest").fetchone()[0]
        
        return {
            "ok": True,
            "worker_last_run_ts": worker_ts,
            "items_db": items_count,
            "db_path": DB_PATH
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()

    return {"ok": False, "error": "Unknown error"} # Should not reach here
