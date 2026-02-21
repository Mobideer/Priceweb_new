import os
import config
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
    
    # Apply performance pragmas to every connection
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass  # pragma: no cover - best effort logging
        
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

        # FTS5 Search Index
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS items_search USING fts5(
                sku UNINDEXED,
                name
            );
        """)

        # Triggers to keep FTS index in sync (idempotent creation)
        existing_triggers = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}

        if 'items_latest_ai' not in existing_triggers:
            conn.execute("""
                CREATE TRIGGER items_latest_ai AFTER INSERT ON items_latest BEGIN
                    INSERT INTO items_search(sku, name) VALUES (new.sku, COALESCE(new.name, ''));
                END;
            """)
        
        if 'items_latest_ad' not in existing_triggers:
            conn.execute("""
                CREATE TRIGGER items_latest_ad AFTER DELETE ON items_latest BEGIN
                    DELETE FROM items_search WHERE sku = old.sku;
                END;
            """)
        
        if 'items_latest_au' not in existing_triggers:
            conn.execute("""
                CREATE TRIGGER items_latest_au AFTER UPDATE ON items_latest BEGIN
                    DELETE FROM items_search WHERE sku = old.sku;
                    INSERT INTO items_search(sku, name) VALUES (new.sku, COALESCE(new.name, ''));
                END;
            """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                k TEXT PRIMARY KEY,
                v TEXT
            );
        """)

        # Population (only if empty to avoid duplicates on every run)
        search_count = conn.execute("SELECT COUNT(*) FROM items_search").fetchone()[0]
        if search_count == 0:
            log_msg = "Populating items_search from items_latest..."
            print(log_msg)
            conn.execute("INSERT INTO items_search(sku, name) SELECT sku, name FROM items_latest;")
        
        conn.commit()
    finally:
        conn.close()

def load_existing_latest(conn: sqlite3.Connection) -> Dict[str, Tuple]:
    try:
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
    finally:
        if 'cur' in locals():
            cur.close()
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

def get_meta_value(conn: sqlite3.Connection, key: str) -> Optional[str]:
    r = conn.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
    return r[0] if r else None

def set_meta_value(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT INTO meta(k,v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, value))
