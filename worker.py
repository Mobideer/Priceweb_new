import os
from dotenv import load_dotenv
load_dotenv()
import json
import time
import requests
import ijson
import db
import notify
import config

JSON_URL = os.environ.get("PRICE_JSON_URL", "https://app.price-matrix.ru/WebApi/SummaryExportLatestGet/v2-202010181100-IWYHBWQFVQEMXNPVUNRAULOGYTDTUMMSUEPYBCIWMPYUMVYQLP")
SNAPSHOT_RETENTION_DAYS = int(os.environ.get("SNAPSHOT_RETENTION_DAYS", "15"))
LOCAL_DATA_FILE = "data/last_catalog_download.json"

LOG_PATH = config.get_log_path()

def log_with_timestamp(message):
    """Print message with timestamp in Moscow timezone and write to log file."""
    try:
        import pytz
        from datetime import datetime
        tz = pytz.timezone('Europe/Moscow')
        timestamp = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    except:
        # Fallback if pytz is not available
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    
    log_msg = f"[{timestamp}] {message}"
    print(log_msg)
    
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(log_msg + "\n")
    except Exception as e:
        print(f"Failed to write to log file: {e}")

def get_exchange_rates():
    """Fetches current exchange rates (USD, EUR -> RUB) with fallbacks."""
    rates = {"USD": 92.0, "EUR": 100.0, "RUB": 1.0}
    try:
        # Using a reliable public API
        resp = requests.get("https://open.er-api.com/v6/latest/USD", timeout=10)
        if resp.ok:
            data = resp.json()
            usd_rub = data.get("rates", {}).get("RUB")
            if usd_rub:
                rates["USD"] = usd_rub
                # Get EUR through USD cross rate
                eur_usd = data.get("rates", {}).get("EUR")
                if eur_usd:
                    rates["EUR"] = usd_rub / eur_usd
    except Exception as e:
        log_with_timestamp(f"Warning: Could not fetch real-time exchange rates: {e}. Using default values.")
    return rates

def download_if_needed(conn):
    """Downloads the JSON file only if it has changed, using ETag/Last-Modified."""
    etag = db.get_meta_value(conn, 'last_etag')
    mtime = db.get_meta_value(conn, 'last_modified')
    
    headers = {}
    if etag: headers['If-None-Match'] = etag
    if mtime: headers['If-Modified-Since'] = mtime
    
    # Ensure dir exists
    os.makedirs(os.path.dirname(LOCAL_DATA_FILE), exist_ok=True)
    
    log_with_timestamp(f"Checking for updates from URL: {JSON_URL}")
    with requests.get(JSON_URL, headers=headers, stream=True, timeout=300) as resp:
        if resp.status_code == 304:
            log_with_timestamp("Server returned 304 Not Modified. Using cached file.")
            return False, etag, mtime
        
        resp.raise_for_status()
        
        resp.raise_for_status()
        
        # Download to temp file first, using PID to avoid collisions
        tmp_file = f"{LOCAL_DATA_FILE}.tmp.{os.getpid()}"
        log_with_timestamp(f"Downloading new data to {tmp_file}...")
        
        try:
            with open(tmp_file, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
                    
            # Move atomically (replace)
            # If the destination file is owned by root and we are not root, this might fail too?
            # But the primary issue is usually opening the temp file for writing if it already exists as root.
            os.replace(tmp_file, LOCAL_DATA_FILE)
        except PermissionError:
            log_with_timestamp(f"Permission denied when writing/moving {tmp_file}. Trying to remove old temp file...")
            try:
                if os.path.exists(tmp_file):
                    os.remove(tmp_file)
            except:
                pass
            raise
        except Exception:
            # Cleanup on failure
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            raise
        
        new_etag = resp.headers.get('ETag')
        new_mtime = resp.headers.get('Last-Modified')
        
        log_with_timestamp(f"Download complete. Size: {os.path.getsize(LOCAL_DATA_FILE) / 1024 / 1024:.1f} MB")
        return True, new_etag, new_mtime

def process_single_product(p, rates):
    """Parses a single product dictionary and returns the item record with prices in RUB."""
    sku = str(p.get('sku', '')).strip()
    if not sku:
        return None
        
    name = p.get('name', 'Unknown')
    
    try:
        our_price = float(p.get('price', 0))
    except:
        our_price = 0.0
        
    try:
        our_qty = float(p.get('quantity', 0))
    except:
        our_qty = 0.0
        
    my_sklad_price = 0.0
    my_sklad_qty = 0.0
    
    suppliers_data = []
    min_p_rub = None
    min_q = None
    min_sup_name = None
    
    raw_suppliers = p.get('suppliers', [])
    if raw_suppliers is None:
        raw_suppliers = []
        
    for s in raw_suppliers:
        s_name = (s.get('name') or 'Unknown').strip()
        s_prod = s.get('product', {})
        
        if not s_prod:
            continue
            
        try:
            raw_p = float(s_prod.get('price', 0))
        except:
            raw_p = 0.0
            
        try:
            qty = float(s_prod.get('quantity', 0))
        except:
            qty = 0.0
            
        currency = s_prod.get('currency', 'RUB').upper()
        # Convert to RUB
        rate = rates.get(currency, 1.0)
        price_rub = raw_p * rate
        
        sup_sku = s_prod.get('sku', '')
        sup_prod_name = s_prod.get('name', '')
        
        if s_name.lower() == "мой склад":
            my_sklad_price = price_rub
            my_sklad_qty = qty
            
        suppliers_data.append({
            "supplier": s_name,
            "price": round(price_rub, 2),
            "original_price": raw_p,
            "currency": currency,
            "qty": qty,
            "supplier_sku": sup_sku,
            "product_name": sup_prod_name
        })
        
        if s_name.lower() != "мой склад" and price_rub > 0 and qty > 0:
            if min_p_rub is None or price_rub < min_p_rub:
                min_p_rub = price_rub
                min_q = qty
                min_sup_name = s_name

    return {
        "sku": sku,
        "name": name,
        "our_price": our_price,
        "our_qty": our_qty,
        "my_sklad_price": round(my_sklad_price, 2),
        "my_sklad_qty": my_sklad_qty,
        "min_sup_price": round(min_p_rub, 2) if min_p_rub else None,
        "min_sup_qty": min_q,
        "min_sup_supplier": min_sup_name,
        "suppliers": suppliers_data
    }

def rotate_snapshots(conn, now_ts):
    cutoff = now_ts - SNAPSHOT_RETENTION_DAYS * 86400
    conn.execute("DELETE FROM item_snapshots WHERE ts < ?", (cutoff,))

def vacuum_db():
    # VACUUM must run on a clean connection without any open transactions.
    try:
        db_path = os.environ.get("PRICE_DB_PATH", "data/priceweb.db")
        if os.path.exists(db_path):
            db_size = os.path.getsize(db_path)
            
            # Simple check for disk space if possible
            try:
                import shutil
                total, used, free = shutil.disk_usage(os.path.dirname(os.path.abspath(db_path)) or ".")
                if free < (db_size * 1.5):
                    log_with_timestamp(f"Skipping VACUUM: insufficient free space ({free/(1024*1024):.1f}MB free, need ~{db_size*1.5/(1024*1024):.1f}MB)")
                    return
            except:
                pass

        # Give a small moment for other connections to truly finalize
        time.sleep(0.5)
        conn = db.get_connection()
        conn.isolation_level = None  # Autocommit mode
        log_with_timestamp("Reclaiming storage space (VACUUM)...")
        conn.execute("VACUUM")
        conn.close()
    except Exception as e:
        log_with_timestamp(f"Warning: VACUUM failed: {e}")

class StatsHelper:
    def __init__(self):
        self.total_count = 0
        self.inserted = 0
        self.changed = 0
        self.snap_added = 0
        self.new_item_names = []
        self.sharp_changes = []
        self.seen_skus = set()

def process_item_loop(p, rates, ts, existing, cur_upsert, cur_snap, stats):
    stats.total_count += 1
    if stats.total_count % 1000 == 0:
        log_with_timestamp(f"Processed {stats.total_count} items...")

    it = process_single_product(p, rates)
    if not it:
        return

    sku = it['sku']
    stats.seen_skus.add(sku)
    supp_json = json.dumps(it['suppliers'], ensure_ascii=False)
    
    curr_vals = (
        it['name'], supp_json, 
        it['our_price'], it['our_qty'],
        it['my_sklad_price'], it['my_sklad_qty'],
        it['min_sup_price'], it['min_sup_qty'], it['min_sup_supplier']
    )
    
    prev = existing.get(sku)
    is_new = prev is None
    # Let's compare only the first 9 elements (sku data)
    is_changed = (prev[:9] != curr_vals) if not is_new else True
    
    if is_new or is_changed:
        cur_snap.execute("""
            INSERT INTO item_snapshots 
            (sku, ts, our_price, our_qty, my_sklad_price, my_sklad_qty, 
             min_sup_price, min_sup_qty, min_sup_supplier)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sku, ts, 
            it['our_price'], it['our_qty'],
            it['my_sklad_price'], it['my_sklad_qty'],
            it['min_sup_price'], it['min_sup_qty'], it['min_sup_supplier']
        ))
        stats.snap_added += 1
    
    if is_new:
        cur_upsert.execute("""
            INSERT INTO items_latest 
            (sku, name, our_price, our_qty, my_sklad_price, my_sklad_qty,
             min_sup_price, min_sup_qty, min_sup_supplier, suppliers_json, updated_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (sku, it['name'], it['our_price'], it['our_qty'],
              it['my_sklad_price'], it['my_sklad_qty'],
              it['min_sup_price'], it['min_sup_qty'], it['min_sup_supplier'],
              supp_json, ts, ts))
        stats.inserted += 1
        if len(stats.new_item_names) < 10:
            stats.new_item_names.append(it['name'])
    elif is_changed:
        cur_upsert.execute("""
            UPDATE items_latest SET
            name=?, our_price=?, our_qty=?, 
            my_sklad_price=?, my_sklad_qty=?,
            min_sup_price=?, min_sup_qty=?, min_sup_supplier=?,
            suppliers_json=?, updated_at=?
            WHERE sku=?
        """, (it['name'], it['our_price'], it['our_qty'],
              it['my_sklad_price'], it['my_sklad_qty'],
              it['min_sup_price'], it['min_sup_qty'], it['min_sup_supplier'],
              supp_json, ts, sku))
        stats.changed += 1
        
        # Check for sharp price changes (only if not new)
        try:
            # Check min_sup_price
            old_min = float(prev[7] if prev[7] is not None else 0) # index 7 is min_sup_price
            new_min = float(it['min_sup_price'] if it['min_sup_price'] is not None else 0)
            
            if old_min > 0 and new_min > 0:
                diff_pct = (new_min - old_min) / old_min * 100.0
                if abs(diff_pct) >= 30.0:
                    stats.sharp_changes.append({
                        "name": it['name'],
                        "sku": sku,
                        "old_price": old_min,
                        "new_price": new_min,
                        "diff_pct": diff_pct,
                        "type": "min_price"
                    })
                    
            # Check our_price
            old_our = float(prev[3] if prev[3] is not None else 0) # index 3 is our_price
            new_our = float(it['our_price'] if it['our_price'] is not None else 0)
            
            if old_our > 0 and new_our > 0:
                 diff_pct_our = (new_our - old_our) / old_our * 100.0
                 if abs(diff_pct_our) >= 30.0:
                      # Avoid duplicates if min_price already added? No, track separately if needed.
                      # But let's prioritize min_price if both changed, or just add both.
                      # Actually, let's just add it if it wasn't already added for min_price
                      # or better, just treat them as separate events if needed.
                      # For simplicity and to avoid spam, we'll just add it to the list.
                      stats.sharp_changes.append({
                        "name": it['name'],
                        "sku": sku,
                        "old_price": old_our,
                        "new_price": new_our,
                        "diff_pct": diff_pct_our,
                        "type": "our_price"
                    })
        except Exception as e:
            # Don't fail the worker for this
            pass
def run():
    host = os.uname().nodename
    notify.notify_start(host)
    t0 = time.time()
    
    rates = get_exchange_rates()
    log_with_timestamp(f"Current Rates: {rates}")
    
    stats_helper = StatsHelper()
    ts = int(time.time())

    try:
        db.ensure_schema()
        conn = db.get_connection()
        conn.isolation_level = None # Autocommit mode for explicit transactions
        try:
            # Step 1: Download
            changed, new_etag, new_mtime = download_if_needed(conn)
            
            # Check if we even need to process
            last_processed_etag = db.get_meta_value(conn, 'proc_etag')
            last_processed_mtime = db.get_meta_value(conn, 'proc_mtime')
            
            if not changed and last_processed_etag == new_etag and last_processed_mtime == new_mtime:
                log_with_timestamp("File is identical and was already processed successfully. Skipping loop.")
                
                # Still prepare stats for notification
                final_stats = {"total": 0, "status": "skipped (no changes)", "duration": time.time() - t0}
                try:
                    db_st = db.get_db_status()
                    final_stats["items_db"] = db_st.get("items_db", 0)
                    db_path = db_st.get("db_path", "priceweb.db")
                    if os.path.exists(db_path):
                        final_stats["db_size_mb"] = os.path.getsize(db_path) / (1024 * 1024)
                except:
                    pass
                
                notify.notify_success(final_stats)
                return

            log_with_timestamp("Loading existing data for comparison...")
            existing = db.load_existing_latest(conn)
            
            log_with_timestamp("Starting transaction...")
            conn.execute("BEGIN TRANSACTION")
            cur_upsert = conn.cursor()
            cur_snap = conn.cursor()

            log_with_timestamp(f"Processing items from {LOCAL_DATA_FILE}...")
            if not os.path.exists(LOCAL_DATA_FILE):
                raise FileNotFoundError(f"Local data file {LOCAL_DATA_FILE} missing after download attempt.")
                
            with open(LOCAL_DATA_FILE, 'r', encoding='utf-8') as f:
                objects = ijson.items(f, 'catalog.item.products.item')
                for p in objects:
                    process_item_loop(p, rates, ts, existing, cur_upsert, cur_snap, stats_helper)

            log_with_timestamp("Rotating snapshots...")
            rotate_snapshots(conn, ts)
            
            log_with_timestamp("Updating meta...")
            db.set_meta_value(conn, 'last_reload_ts', str(ts))
            if new_etag: db.set_meta_value(conn, 'last_etag', new_etag)
            if new_mtime: db.set_meta_value(conn, 'last_modified', new_mtime)
            # Store that we processed this specific version successfully
            if new_etag: db.set_meta_value(conn, 'proc_etag', new_etag)
            if new_mtime: db.set_meta_value(conn, 'proc_mtime', new_mtime)
            
            log_with_timestamp("Committing transaction...")
            conn.execute("COMMIT")
            
            # Explicitly close cursors
            cur_upsert.close()
            cur_snap.close()
            conn.close()
            
            if 'f' in locals():
                f.close()

            # Run vacuum ONLY if something actually changed and we have a clean status
            # This prevents infinite loops of vacuum failing on a full disk when nothing is even happening
            if stats_helper.inserted > 0 or stats_helper.changed > 0 or stats_helper.snap_added > 0:
                vacuum_db()

            stats = {
                "total": stats_helper.total_count,
                "inserted": stats_helper.inserted,
                "changed": stats_helper.changed,
                "snapshots_added": stats_helper.snap_added,
                "duration": time.time() - t0,
                "new_items": stats_helper.new_item_names
            }
            
            # Add DB stats
            try:
                db_st = db.get_db_status()
                stats["items_db"] = db_st.get("items_db", 0)
                db_path = db_st.get("db_path", "priceweb.db")
                if os.path.exists(db_path):
                    stats["db_size_mb"] = os.path.getsize(db_path) / (1024 * 1024)
            except:
                pass

            notify.notify_success(stats)
            
            # Notify about sharp price changes
            if stats_helper.sharp_changes:
                log_with_timestamp(f"Found {len(stats_helper.sharp_changes)} sharp price changes. Sending notification...")
                notify.notify_price_changes(stats_helper.sharp_changes)

            # Check for missing items (deleted from feed)
            if existing: # Only check if we had existing items
                missing_skus = set(existing.keys()) - stats_helper.seen_skus
                if missing_skus:
                    log_with_timestamp(f"Found {len(missing_skus)} missing items (present in DB but not in feed).")
                    missing_items_list = []
                    for sku in missing_skus:
                        # existing[sku] is (name, suppliers_json, ...)
                        # Based on db.load_existing_latest, index 0 is name
                        name = existing[sku][0]
                        missing_items_list.append({"sku": sku, "name": name})
                    
                    # Save to file for bot to handle
                    missing_file = "data/missing_items.json"
                    try:
                        with open(missing_file, 'w', encoding='utf-8') as f:
                            json.dump(missing_items_list, f, ensure_ascii=False, indent=2)
                        
                        notify.notify_missing_items(missing_items_list)
                    except Exception as e:
                        log_with_timestamp(f"Failed to save missing items: {e}")
                
            log_with_timestamp(json.dumps(stats))

        finally:
            conn.close()

    except Exception as e:
        import traceback
        err_msg = traceback.format_exc()
        log_with_timestamp(f"Worker crashed:\n{err_msg}")
        last_part = err_msg[-200:] if len(err_msg) > 200 else err_msg
        notify.notify_fail(f"Worker Error:\n{str(e)}\n\nTraceback summary:\n{last_part}")
        raise
    finally:
        pass

if __name__ == "__main__":
    run()
