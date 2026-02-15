import os
import json
import time
import sqlite3
import threading
import config
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

try:
    import pytz
except ImportError:
    pytz = None

from flask import Flask, render_template, request, jsonify, abort, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user

import db

# Ensure database schema is up to date on start (runs even under gunicorn)
db.ensure_schema()

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "super-secret-price-matrix-key")

APP_VERSION = "1.3.0"  # Stable release: UI polishes, color coding, and cleanup

# --- Authentication ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

class User(UserMixin):
    def __init__(self, id):
        self.id = id

@login_manager.user_loader
def load_user(user_id):
    if user_id == "priceuser":
        return User(user_id)
    return None

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        if username == "priceuser" and password == "priceuser":
            user = User(username)
            login_user(user)
            return redirect(url_for('index'))
        else:
            flash('Неверный логин или пароль')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

# --- Filters ---
@app.template_filter('urlencode')
def urlencode_filter(s):
    if s is None:
        return ""
    return quote_plus(str(s))

@app.template_filter('strip')
def strip_filter(s):
    return str(s or "").strip()

@app.template_filter('format_ts')
def format_ts_filter(ts):
    if not ts:
        return ""
    try:
        tz_name = os.environ.get("TZ", "Europe/Moscow")
        if pytz:
            tz = pytz.timezone(tz_name)
            # datetime.fromtimestamp using UTC and then converting to target TZ
            return datetime.fromtimestamp(int(ts), tz=pytz.UTC).astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
        else:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts)))
    except:
        return str(ts)

@app.template_filter('fromjson')
def fromjson_filter(s):
    return json.loads(s)

# --- Helpers ---

def _get_status():
    st = db.get_db_status()
    st["version"] = APP_VERSION
    return st

def _search_items(q: str, limit: int = 20):
    q = (q or "").strip().lower()
    conn = db.get_connection()
    try:
        if not q:
            # Show newest items by default
            rows = conn.execute("SELECT * FROM items_latest ORDER BY created_at DESC, sku ASC LIMIT ?", (limit,)).fetchall()
            return {"items": [_augment_item_with_stats(dict(r)) for r in rows]}
            
        # Simple search matching logic
        # Priority 1: Exact SKU
        rows = conn.execute("SELECT * FROM items_latest WHERE lower(sku) = ? ORDER BY created_at DESC, sku ASC", (q,)).fetchall()
        
        if not rows:
            # Priority 2: SKU starts with
            rows = conn.execute("SELECT * FROM items_latest WHERE lower(sku) LIKE ? ORDER BY created_at DESC, sku ASC LIMIT ?", (q + '%', limit)).fetchall()
        
        if len(rows) < limit:
            # Priority 3: Multi-word FTS Search (if query has multiple words or no SKU match)
            # This handles both "word1 word2" and single words not matching SKU exactly
            rem = limit - len(rows)
            found_skus = {r['sku'] for r in rows}
            
            tokens = [t for t in q.split() if t]
            if tokens:
                # 1. Try FTS5 Search first (High performance)
                # Use prefix matching on every token and explicit AND
                fts_query = " AND ".join([f"{t}*" for t in tokens])
                
                placeholders = ",".join("?" * len(found_skus)) if found_skus else "''"
                sql_fts = f"""
                    SELECT i.* FROM items_latest i
                    JOIN items_search s ON i.rowid = s.rowid
                    WHERE s.items_search MATCH ?
                      AND i.sku NOT IN ({placeholders})
                    ORDER BY i.created_at DESC, i.sku ASC
                    LIMIT ?
                """
                try:
                    cursor = conn.execute(sql_fts, (fts_query, *found_skus, rem))
                    fts_results = cursor.fetchall()
                    rows.extend(fts_results)
                    found_skus.update({r['sku'] for r in fts_results})
                    rem = limit - len(rows)
                except Exception as e:
                    print(f"FTS5 Search Error: {e}")

                # 2. Fallback to LIKE if we still need more results
                # This ensures we find items even if FTS5 has issues with certain characters
                if rem > 0:
                    like_clauses = []
                    params = list(found_skus)
                    for t in tokens:
                        like_clauses.append("(lower(i.sku) LIKE ? OR lower(i.name) LIKE ?)")
                        params.append(f"%{t}%")
                        params.append(f"%{t}%")
                    
                    placeholders = ",".join("?" * len(found_skus)) if found_skus else "''"
                    sql_like = f"""
                        SELECT * FROM items_latest i
                        WHERE i.sku NOT IN ({placeholders})
                        AND {" AND ".join(like_clauses)}
                        ORDER BY i.created_at DESC, i.sku ASC
                        LIMIT ?
                    """
                    params.append(rem)
                    cursor = conn.execute(sql_like, params)
                    rows.extend(cursor.fetchall())
        
        return {"items": [_augment_item_with_stats(dict(r)) for r in rows]}
    finally:
        conn.close()

def _augment_item_with_stats(item_dict):
    """Adds supplier stats (available/total) to the item dictionary."""
    try:
        sups = json.loads(item_dict.get('suppliers_json', '[]'))
        total = 0
        in_stock = 0
        for s in sups:
            name = s.get('supplier', '').strip().lower()
            if not name or name == 'мой склад':
                continue
            total += 1
            if float(s.get('qty', 0)) > 0:
                in_stock += 1
        # Always return a string to distinguish from missing field
        item_dict['sup_stats'] = f"({in_stock}/{total})"
    except Exception:
        item_dict['sup_stats'] = "(err)"
    return item_dict

# --- Routes ---

@app.route('/api/search')
def api_search():
    q = request.args.get('q', '').strip()
    limit = request.args.get('limit', 20, type=int)
    results = {"items": []}
    if q:
        results = _search_items(q, limit)
    return jsonify(results)

@app.route('/')
@login_required
def index():
    q = request.args.get('q', '').strip()
    limit = request.args.get('limit', 20, type=int)
    
    status = _get_status()
    results = _search_items(q, limit)

    return render_template('index.html', 
                           q=q, 
                           limit=limit, 
                           items=results['items'], 
                           status=status)

@app.route('/ui/history')
@login_required
def ui_history():
    sku = request.args.get('sku', '').strip()
    days = request.args.get('days', 7, type=int)
    
    if not sku:
        return "SKU required", 400
        
    conn = db.get_connection()
    try:
        cutoff = int(time.time()) - days * 86400
        # Aggregation logic similar to priceweb: min price per day
        rows = conn.execute("""
            SELECT 
                day_date,
                our_price,
                min_sup_price,
                min_sup_supplier
            FROM (
                SELECT 
                    date(ts, 'unixepoch', 'localtime') as day_date,
                    our_price,
                    min_sup_price,
                    min_sup_supplier,
                    ROW_NUMBER() OVER(PARTITION BY date(ts, 'unixepoch', 'localtime') ORDER BY min_sup_price ASC, ts DESC) as rn
                FROM item_snapshots
                WHERE sku = ? AND ts >= ?
            )
            WHERE rn = 1
            ORDER BY day_date ASC
        """, (sku, cutoff)).fetchall()
        
        data = [dict(r) for r in rows]
        return render_template('partials/history.html', sku=sku, items=data, days=days)
    finally:
        conn.close()

# --- Reports ---

@app.route('/reports/spread')
@login_required
def report_spread():
    threshold = request.args.get('threshold', 20.0, type=float)
    per_page = request.args.get('limit', 100, type=int)
    page = request.args.get('page', 1, type=int)
    max_price = request.args.get('max_price', 2000000.0, type=float)
    in_stock_only = request.args.get('in_stock_only', 1, type=int)
    exclude_list = request.args.getlist('exclude')
    exclude_set = {s.lower() for s in exclude_list}
    
    conn = db.get_connection()
    try:
        # Get all distinct supplier names for the filter UI
        all_sups_rows = conn.execute("SELECT suppliers_json FROM items_latest").fetchall()
        suppliers_all = set()
        for r in all_sups_rows:
            try:
                sups = json.loads(r['suppliers_json'])
                for s in sups:
                    name = s.get('supplier', '').strip()
                    if name and name.lower() != 'мой склад':
                        suppliers_all.add(name)
            except: continue
        
        query = f"SELECT sku, name, suppliers_json FROM items_latest WHERE min_sup_price > 0"
        rows = conn.execute(query).fetchall()
        
        results = []
        for r in rows:
            try:
                sups = json.loads(r['suppliers_json'])
            except: continue
                
            valid_sups = []
            for s in sups:
                name = s.get('supplier', '').strip()
                if name.lower() == 'мой склад' or name.lower() in exclude_set:
                    continue
                    
                p = float(s.get('price', 0))
                q = float(s.get('qty', 0))
                if p <= 0: continue
                if in_stock_only and q <= 0: continue
                # Issue 5: Filter garbage prices early in calculation
                if p > max_price: continue
                
                valid_sups.append(s)
            
            if len(valid_sups) < 2:
                continue
                
            prices = [float(s['price']) for s in valid_sups]
            min_p = min(prices)
            max_p = max(prices)
            
            # Re-check max_price on the final spread max to be sure
            if max_p > max_price:
                 continue
            
            spread = (max_p - min_p) * 100.0 / min_p
            if spread < threshold:
                continue
            
            min_s_names = [s['supplier'] for s in valid_sups if float(s['price']) == min_p]
            max_s_names = [s['supplier'] for s in valid_sups if float(s['price']) == max_p]
            
            results.append({
                'sku': r['sku'],
                'name': r['name'],
                'min_price': min_p,
                'min_suppliers': ", ".join(min_s_names),
                'max_price': max_p,
                'max_suppliers': ", ".join(max_s_names),
                'spread_pct': round(spread, 2),
                'suppliers_cnt': len(valid_sups),
                'suppliers_json': r['suppliers_json']
            })
            
        results.sort(key=lambda x: x['spread_pct'], reverse=True)
        
        total_count = len(results)
        total_pages = (total_count + per_page - 1) // per_page if per_page > 0 else 1
        page = max(1, min(page, total_pages))
        
        start = (page - 1) * per_page
        items_slice = results[start:start + per_page]
        
        return render_template('report_spread.html', 
                               items=items_slice, 
                               threshold=threshold, 
                               limit=per_page,
                               page=page,
                               total_pages=total_pages,
                               total_count=total_count,
                               max_price=max_price,
                               in_stock_only=in_stock_only,
                               suppliers_all=sorted(list(suppliers_all)),
                               exclude_set=exclude_set,
                               exclude_list=exclude_list)
    finally:
        conn.close()

@app.route('/reports/markup')
@login_required
def report_markup():
    markup_pct = request.args.get('markup_pct', 10.0, type=float)
    per_page = request.args.get('limit', 100, type=int)
    page = request.args.get('page', 1, type=int)
    max_price = request.args.get('max_price', 2000000.0, type=float)
    in_stock_only = request.args.get('in_stock_only', 1, type=int)
    qty_equal = request.args.get('qty_equal', 0, type=int)
    exclude_list = request.args.getlist('exclude')
    exclude_set = {s.lower() for s in exclude_list}
    
    conn = db.get_connection()
    try:
        # Get all distinct supplier names
        all_sups_rows = conn.execute("SELECT suppliers_json FROM items_latest").fetchall()
        suppliers_all = set()
        for r in all_sups_rows:
            try:
                sups = json.loads(r['suppliers_json'])
                for s in sups:
                    name = s.get('supplier', '').strip()
                    if name and name.lower() != 'мой склад':
                        suppliers_all.add(name)
            except: continue

        query = "SELECT * FROM items_latest WHERE our_price > 0"
        rows = conn.execute(query).fetchall()
        
        results = []
        for r in rows:
            our = r['our_price']
            our_qty = r['our_qty']
            
            try:
                sups = json.loads(r['suppliers_json'])
            except: continue
                
            filtered_prices = []
            filtered_sups = []
            for s in sups:
                name = s.get('supplier', '').strip()
                if name.lower() == 'мой склад' or name.lower() in exclude_set:
                    continue
                
                p = float(s.get('price', 0))
                q = float(s.get('qty', 0))
                
                if p <= 0 or p > max_price: continue
                if in_stock_only and q <= 0: continue
                if qty_equal and q != our_qty: continue
                
                filtered_prices.append(p)
                filtered_sups.append(name)
                
            if not filtered_prices: continue
            
            min_sup = min(filtered_prices)
            min_s_names = [filtered_sups[i] for i, p in enumerate(filtered_prices) if p == min_sup]
            
            our_with_markup = our * (1.0 + markup_pct / 100.0)
            
            if our_with_markup < min_sup:
                delta_abs = min_sup - our_with_markup
                delta_pct = (min_sup / our_with_markup - 1.0) * 100.0 if our_with_markup > 0 else 0
                
                results.append({
                    'sku': r['sku'],
                    'name': r['name'],
                    'our_price': our,
                    'our_qty': our_qty,
                    'min_sup_price': min_sup,
                    'min_suppliers': ", ".join(min_s_names),
                    'our_price_with_markup': round(our_with_markup, 2),
                    'delta_abs': round(delta_abs, 2),
                    'delta_pct': round(delta_pct, 2),
                    'suppliers_json': r['suppliers_json']
                })
            
        results.sort(key=lambda x: x['delta_abs'], reverse=True)
        
        total_count = len(results)
        total_pages = (total_count + per_page - 1) // per_page if per_page > 0 else 1
        page = max(1, min(page, total_pages))
        
        start = (page - 1) * per_page
        items_slice = results[start:start+per_page]
        
        # Augment ONLY the visible slice with stats
        for item in items_slice:
            _augment_item_with_stats(item)
        
        return render_template('report_markup.html',
                               items=items_slice,
                               markup_pct=markup_pct,
                               limit=per_page,
                               page=page,
                               total_pages=total_pages,
                               total_count=total_count,
                               max_price=max_price,
                               in_stock_only=in_stock_only,
                               qty_equal=qty_equal,
                               suppliers_all=sorted(list(suppliers_all)),
                               exclude_set=exclude_set,
                               exclude_list=exclude_list)
    finally:
        conn.close()

@app.route('/api/reload')
def api_reload():
    token = request.args.get('token', '')
    expected_token = os.environ.get("RELOAD_TOKEN", "")
    
    print(f"[API] Reload requested. Token present: {bool(token)}, Matches: {token == expected_token}")
    
    # Allow if token matches OR user is logged in
    if not (expected_token and token == expected_token) and not current_user.is_authenticated:
        if not expected_token:
            return jsonify({"ok": False, "error": "RELOAD_TOKEN not set in /etc/priceweb_new.env"}), 403
        return jsonify({"ok": False, "error": f"Unauthorized (expected length {len(expected_token)})"}), 403
    
    def run_worker():
        log_path = config.get_log_path()
        def _log_api(msg):
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [API_REL] {msg}\n")

        try:
            _log_api("Starting background worker subprocess...")
            import sys
            import subprocess
            
            # Explicitly pass environment and set CWD
            cwd = os.path.dirname(os.path.abspath(__file__))
            res = subprocess.run(
                [sys.executable, "worker.py"], 
                capture_output=True, 
                text=True, 
                env=os.environ.copy(),
                cwd=cwd
            )
            if res.returncode == 0:
                _log_api("Background worker finished successfully.")
            else:
                _log_api(f"Background worker FAILED with code {res.returncode}.")
                if res.stderr:
                    _log_api(f"Worker Stderr: {res.stderr}")
        except Exception as e:
            _log_api(f"Unexpected error in background worker thread: {e}")

    try:
        thread = threading.Thread(target=run_worker)
        thread.daemon = True
        thread.start()
        return jsonify({"ok": True, "message": "Воркер запущен в фоновом режиме. Следите за логами."})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route('/api/debug_env')
@login_required # Security: only for logged in users
def api_debug_env():
    try:
        # Only show names of sensitive vars
        keys = list(os.environ.keys())
        important_keys = ["TG_BOT_TOKEN", "TG_CHAT_ID", "RELOAD_TOKEN", "PRICE_DB_PATH", "PORT"]
        details = {k: ("SET (len=" + str(len(os.environ[k])) + ")" if k in os.environ else "MISSING") for k in important_keys}
        return jsonify({
            "all_keys_count": len(keys),
            "important_vars": details,
            "config_loaded": config.load_config() # Re-check
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route('/api/logs')
@login_required
def api_logs():
    log_path = config.get_log_path()
    
    try:
        if not os.path.exists(log_path):
            return jsonify({"ok": True, "logs": "Лог-файл пока не создан. Нажмите 'Reload Data', чтобы запустить воркер и создать логи."})

        
        with open(log_path, 'r') as f:
            # Get last 100 lines
            lines = f.readlines()
            last_lines = lines[-100:]
            return jsonify({"ok": True, "logs": "".join(last_lines)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5002))
    app.run(host='0.0.0.0', port=port, debug=True)
