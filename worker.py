import os
import json
import time
import requests
import ijson
import db
import notify

JSON_URL = os.environ.get("PRICE_JSON_URL", "https://app.price-matrix.ru/WebApi/SummaryExportLatestGet/v2-202010181100-IWYHBWQFVQEMXNPVUNRAULOGYTDTUMMSUEPYBCIWMPYUMVYQLP")
SNAPSHOT_RETENTION_DAYS = int(os.environ.get("SNAPSHOT_RETENTION_DAYS", "30"))
LOCAL_DATA_FILE = "data.json"

def process_single_product(p):
    """Parses a single product dictionary from the JSON and returns the item record."""
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
    min_p = None
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
            price = float(s_prod.get('price', 0))
        except:
            price = 0.0
            
        try:
            qty = float(s_prod.get('quantity', 0))
        except:
            qty = 0.0
            
        currency = s_prod.get('currency', 'RUB')
        sup_sku = s_prod.get('sku', '')
        sup_prod_name = s_prod.get('name', '')
        
        # Issue 2: MySklad data source
        if s_name.lower() == "мой склад":
            my_sklad_price = price
            my_sklad_qty = qty
            
        # Add to list
        suppliers_data.append({
            "supplier": s_name,
            "price": price,
            "qty": qty,
            "currency": currency,
            "supplier_sku": sup_sku,
            "product_name": sup_prod_name
        })
        
        # Calc min price (conceptually) - Exclude "Мой склад" from competitor calculations? 
        # Usually competitors are other suppliers. Priceweb usually excludes MS from min_sup.
        if s_name.lower() != "мой склад" and price > 0 and qty > 0:
            if min_p is None or price < min_p:
                min_p = price
                min_q = qty
                min_sup_name = s_name

    return {
        "sku": sku,
        "name": name,
        "our_price": our_price,
        "our_qty": our_qty,
        "my_sklad_price": my_sklad_price,
        "my_sklad_qty": my_sklad_qty,
        "min_sup_price": min_p,
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
            conn.execute("BEGIN")
            cur_upsert = conn.cursor()
            cur_snap = conn.cursor()

            # Determine source: local file if exists, else fetch URL
            if os.path.exists(LOCAL_DATA_FILE):
                print(f"Reading from local file: {LOCAL_DATA_FILE}")
                f = open(LOCAL_DATA_FILE, 'r', encoding='utf-8')
                # Path depends on JSON structure: {"catalog": [ {"products": [ ... ]} ]}
                # We use ijson to iterate over products
                objects = ijson.items(f, 'catalog.item.products.item')
            else:
                print(f"Fetching from URL: {JSON_URL}")
                resp = requests.get(JSON_URL, stream=True, timeout=120)
                resp.raise_for_status()
                objects = ijson.items(resp.raw, 'catalog.item.products.item')

            for p in objects:
                total_count += 1
                if total_count % 1000 == 0:
                    print(f"Processed {total_count} items...")

                it = process_single_product(p)
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
                is_changed = prev != curr_vals
                
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
                         min_sup_price, min_sup_qty, min_sup_supplier, suppliers_json, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (sku, it['name'], it['our_price'], it['our_qty'],
                          it['my_sklad_price'], it['my_sklad_qty'],
                          it['min_sup_price'], it['min_sup_qty'], it['min_sup_supplier'],
                          supp_json, ts))
                    inserted += 1
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
            
            # Close file if opened
            if 'f' in locals():
                f.close()

            stats = {
                "total": total_count,
                "inserted": inserted,
                "changed": changed,
                "snapshots_added": snap_added,
                "duration": time.time() - t0
            }
            notify.notify_success(stats)
            print(json.dumps(stats))

        finally:
            conn.close()

    except Exception as e:
        import traceback
        traceback.print_exc()
        notify.notify_fail(str(e))
        raise

if __name__ == "__main__":
    run()
