import os
import json
import time
import requests
import ijson
import db
import notify
import config

JSON_URL = os.environ.get("PRICE_JSON_URL", "https://app.price-matrix.ru/WebApi/SummaryExportLatestGet/v2-202010181100-IWYHBWQFVQEMXNPVUNRAULOGYTDTUMMSUEPYBCIWMPYUMVYQLP")
SNAPSHOT_RETENTION_DAYS = int(os.environ.get("SNAPSHOT_RETENTION_DAYS", "30"))
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
        
        # Download to temp file first
        tmp_file = LOCAL_DATA_FILE + ".tmp"
        log_with_timestamp(f"Downloading new data to {tmp_file}...")
        
        with open(tmp_file, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
                
        os.replace(tmp_file, LOCAL_DATA_FILE)
        
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

def process_item_loop(p, rates, ts, existing, cur_upsert, cur_snap, stats):
    stats.total_count += 1
    if stats.total_count % 1000 == 0:
        log_with_timestamp(f"Processed {stats.total_count} items...")

    it = process_single_product(p, rates)
    if not it:
        return

    sku = it['sku']
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
        # Run vacuum strictly outside the main connection life-cycle
        try:
            vacuum_db()
        except:
            pass

if __name__ == "__main__":
    run()
