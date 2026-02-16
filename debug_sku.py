import db
import sys
import json
import os
from datetime import datetime

def format_ts(ts):
    if not ts: return "N/A"
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

def check_sku(sku):
    print(f"--- Debugging SKU: {sku} ---")
    
    conn = db.get_connection()
    
    # Check items_latest
    print("\n[items_latest]")
    cur = conn.cursor()
    cur.execute("SELECT name, min_sup_price, min_sup_supplier, updated_at, suppliers_json FROM items_latest WHERE sku = ?", (sku,))
    row = cur.fetchone()
    
    if row:
        print(f"Name: {row[0]}")
        print(f"Min Price: {row[1]}")
        print(f"Supplier: {row[2]}")
        print(f"Updated: {format_ts(row[3])}")
        try:
            suppliers = json.loads(row[4])
            print("Suppliers Data:")
            for s in suppliers:
                print(f"  - {s.get('supplier')}: {s.get('price')} {s.get('currency')} (Qty: {s.get('qty')})")
        except:
            print("  (Bad JSON)")
    else:
        print("Not found in items_latest")

    # Check item_snapshots
    print("\n[item_snapshots (Last 10)]")
    cur.execute("SELECT ts, min_sup_price, min_sup_supplier FROM item_snapshots WHERE sku = ? ORDER BY ts DESC LIMIT 10", (sku,))
    rows = cur.fetchall()
    for r in rows:
        print(f"{format_ts(r[0])}: Price={r[1]}, Supplier={r[2]}")
        
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python debug_sku.py <SKU>")
        sys.exit(1)
    
    check_sku(sys.argv[1])
