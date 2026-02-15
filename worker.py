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
LOCAL_DATA_FILE = "data.json"

LOG_PATH = os.environ.get("PRICE_LOG_PATH", "cron_log.log")

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

def run():
    host = os.uname().nodename
    notify.notify_start(host)
    t0 = time.time()
    
    rates = get_exchange_rates()
    log_with_timestamp(f"Current Rates: {rates}")
    
    total_count = 0
    inserted = 0
    changed = 0
    snap_added = 0
    ts = int(time.time())

    try:
        db.ensure_schema()
        conn = db.get_connection()
        try:
            existing = db.load_existing_latest(conn)
            new_item_names = []
            conn.execute("BEGIN")
            cur_upsert = conn.cursor()
            cur_snap = conn.cursor()

            if os.path.exists(LOCAL_DATA_FILE):
                log_with_timestamp(f"Reading from local file: {LOCAL_DATA_FILE}")
                f = open(LOCAL_DATA_FILE, 'r', encoding='utf-8')
                objects = ijson.items(f, 'catalog.item.products.item')
            else:
                log_with_timestamp(f"Fetching from URL: {JSON_URL}")
                resp = requests.get(JSON_URL, stream=True, timeout=300)
                resp.raise_for_status()
                objects = ijson.items(resp.raw, 'catalog.item.products.item')

            for p in objects:
                total_count += 1
                if total_count % 1000 == 0:
                    log_with_timestamp(f"Processed {total_count} items...")

                it = process_single_product(p, rates)
                if not it:
                    continue

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
                # curr_vals has 9 elements, prev in old schema had 9, 
                # but load_existing_latest now returns 10 (incl created_at)
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
                    snap_added += 1
                
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
                    inserted += 1
                    if len(new_item_names) < 10:
                        new_item_names.append(it['name'])
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
                    changed += 1

            rotate_snapshots(conn, ts)
            conn.execute("INSERT INTO meta(k,v) VALUES('last_reload_ts', ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(ts),))
            conn.commit()
            
            if 'f' in locals():
                f.close()

            stats = {
                "total": total_count,
                "inserted": inserted,
                "changed": changed,
                "snapshots_added": snap_added,
                "duration": time.time() - t0,
                "new_items": new_item_names
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
        traceback.print_exc()
        notify.notify_fail(str(e))
        raise

if __name__ == "__main__":
    run()
