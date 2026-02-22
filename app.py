import os
from dotenv import load_dotenv
load_dotenv()
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
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from pydantic import BaseModel, Field, ValidationError

import db

# Ensure database schema is up to date on start (runs even under gunicorn)
db.ensure_schema()

# --- Request Validation Schemas ---
class SearchSchema(BaseModel):
    q: str = ""
    limit: int = Field(default=20, ge=1, le=500)
    page: int = Field(default=1, ge=1)
    sort_by: str = "created_at"
    sort_asc: bool = False
    our_price: Optional[str] = None
    our_qty: Optional[str] = None
    my_sklad_price: Optional[str] = None
    my_sklad_qty: Optional[str] = None
    min_sup_price: Optional[str] = None
    min_sup_supplier: Optional[str] = None

class HistorySchema(BaseModel):
    sku: str
    days: int = Field(default=7, ge=1)

class SpreadReportSchema(BaseModel):
    threshold: float = Field(default=20.0, ge=0)
    limit: int = Field(default=100, ge=1, le=500)
    page: int = Field(default=1, ge=1)
    max_price: float = Field(default=2000000.0, ge=0)
    in_stock_only: int = Field(default=1, ge=0, le=1)
    exclude: List[str] = Field(default_factory=list)

class MarkupReportSchema(BaseModel):
    markup_pct: float = Field(default=10.0, ge=0)
    limit: int = Field(default=100, ge=1, le=500)
    page: int = Field(default=1, ge=1)
    max_price: float = Field(default=2000000.0, ge=0)
    in_stock_only: int = Field(default=1, ge=0, le=1)
    qty_equal: int = Field(default=0, ge=0, le=1)
    exclude: List[str] = Field(default_factory=list)

class ChangesReportSchema(BaseModel):
    days: int = Field(default=7, ge=1)
    threshold: float = Field(default=30.0, ge=0)
    type: str = "all"

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY")
if not app.secret_key:
    raise ValueError("FLASK_SECRET_KEY environment variable is not set")

def get_rate_limit_key():
    # 1. External scripts via API Token
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split(' ', 1)[1]
        return f"token:{token}"
        
    # 2. Web interface via Logged-in User
    if current_user.is_authenticated:
        return f"user:{current_user.id}"
        
    # 3. Fallback for unauthorized/anonymous visitors
    return f"ip:{get_remote_address()}"

# Initialize rate limiter
limiter = Limiter(
    get_rate_limit_key,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)
APP_VERSION = "1.8.4"  # Health Check & Pydantic Validation
# ... (skipped for brevity, but I need to do this in two separate replace calls if they are far apart, they are at line 44 and 143, so doing multi-replace)

@app.context_processor
def inject_version():
    return dict(version=APP_VERSION)

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
        expected_user = os.environ.get("WEB_USERNAME", "priceuser")
        expected_pass = os.environ.get("WEB_PASSWORD", "priceuser")
        if username == expected_user and password == expected_pass:
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
    except (ValueError, TypeError, OSError):
        return str(ts)

@app.template_filter('fromjson')
def fromjson_filter(s):
    return json.loads(s)

# --- Helpers ---

def _get_status():
    st = db.get_db_status()
    st["version"] = APP_VERSION
    return st

def _parse_filter_value(val):
    """Parses a filter string like '>100' into (operator, value)."""
    val = val.strip()
    op = '='
    if val.startswith('>='): op, val = '>=', val[2:]
    elif val.startswith('<='): op, val = '<=', val[2:]
    elif val.startswith('>'): op, val = '>', val[1:]
    elif val.startswith('<'): op, val = '<', val[1:]
    elif val.startswith('='): op, val = '=', val[1:]
    elif val.startswith('!='): op, val = '!=', val[2:]
    elif val.startswith('!'): op, val = '!=', val[1:]
    
    try:
        return op, float(val)
    except (ValueError, TypeError):
        return '=', val # Fallback to string equality if not a number

@app.route('/api/run-worker-external', methods=['GET', 'POST'])
@limiter.limit("5 per minute")
def run_worker_external():
    """Trigger worker via external URL with token."""
    auth_header = request.headers.get('Authorization')
    token = None
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split(' ', 1)[1]
    secret = os.environ.get("WORKER_TOKEN")
    
    if not secret or token != secret:
        return jsonify({"status": "error", "message": "Invalid or missing token"}), 403
    
    try:
        # Run worker in background
        subprocess.Popen([sys.executable, "worker.py"])
        return jsonify({"status": "started"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

def _get_items(q: str = "", limit: int = 20, page: int = 1, sort_by: str = "created_at", sort_asc: bool = False, filters: Dict = None):
    conn = db.get_connection()
    try:
        # Whitelist sort columns to prevent SQL injection
        allowed_sorts = {
            'created_at': 'created_at',
            'updated_at': 'updated_at',
            'sku': 'sku',
            'name': 'name',
            'our_price': 'our_price',
            'our_qty': 'our_qty',
            'my_sklad_price': 'my_sklad_price',
            'my_sklad_qty': 'my_sklad_qty',
            'min_sup_price': 'min_sup_price',
            'min_sup_qty': 'min_sup_qty',
            'min_sup_supplier': 'min_sup_supplier'
        }
        order_col = allowed_sorts.get(sort_by, 'created_at')
        order_dir = "ASC" if sort_asc else "DESC"
        
        where_clauses = []
        params = []
        
        # 1. Text Search (q)
        q = (q or "").strip()
        if q:
            # We use a simple LIKE approach for now to combine with other filters easily
            # FTS matches are hard to combine with complex WHEREs without joining
            # For simplicity and robustness with sorting/filtering:
            tokens = q.split()
            for t in tokens:
                where_clauses.append("(lower(sku) LIKE ? OR lower(name) LIKE ?)")
                params.append(f"%{t}%")
                params.append(f"%{t}%")

        # 2. Filters
        if filters:
            for col, val in filters.items():
                if not val: continue
                val = str(val).strip()
                
                # Special handling for quantity logic embedded in price fields (from UI convention 'q>10')
                # But here we expect the caller to separate them if possible. 
                # If the UI sends 'q>10' as 'our_price', we need to handle it or expect UI to split.
                # Let's assume UI sends specific keys like 'our_qty' if it wants to filter qty.
                
                if col in ['our_price', 'our_qty', 'my_sklad_price', 'my_sklad_qty', 'min_sup_price', 'min_sup_qty']:
                    op, num_val = _parse_filter_value(val)
                    where_clauses.append(f"{col} {op} ?")
                    params.append(num_val)
                elif col == 'min_sup_supplier':
                     # Search within JSON for supplier with case-insensitive fallback for Cyrillic
                     v_lower = val.lower()
                     v_title = val.title()
                     v_upper = val.upper()
                     
                     clause = f"(lower(suppliers_json) LIKE ? OR suppliers_json LIKE ? OR suppliers_json LIKE ?)"
                     where_clauses.append(clause)
                     params.append(f"%{v_lower}%")
                     params.append(f"%{v_title}%")
                     params.append(f"%{v_upper}%")
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        # Count total matches first
        count_query = f"SELECT COUNT(*) FROM items_latest WHERE {where_sql}"
        total_count = conn.execute(count_query, params).fetchone()[0]
        
        # Pagination
        limit = max(1, min(limit, 500)) # Cap limit
        page = max(1, page)
        offset = (page - 1) * limit
        total_pages = (total_count + limit - 1) // limit if limit > 0 else 1

        query = f"""
            SELECT * FROM items_latest
            WHERE {where_sql}
            ORDER BY {order_col} {order_dir}, sku ASC
            LIMIT ? OFFSET ?
        """
        params.append(limit)
        params.append(offset)
        
        rows = conn.execute(query, params).fetchall()
        
        result_items = [_augment_item_with_stats(dict(r)) for r in rows]
        
        return {
            "items": result_items,
            "total_count": total_count,
            "page": page,
            "limit": limit,
            "total_pages": total_pages
        }
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



@app.route('/api/search')
@limiter.limit("30 per minute")
def api_search():
    try:
        args = SearchSchema(**request.args.to_dict())
    except ValidationError as e:
        return jsonify({"ok": False, "error": "Invalid parameters", "details": e.errors()}), 400
        
    filters = {
        'our_price': args.our_price,
        'our_qty': args.our_qty,
        'my_sklad_price': args.my_sklad_price,
        'my_sklad_qty': args.my_sklad_qty,
        'min_sup_price': args.min_sup_price,
        'min_sup_supplier': args.min_sup_supplier
    }
    
    results = _get_items(args.q.strip(), args.limit, args.page, args.sort_by, args.sort_asc, filters)
    return jsonify(results)

@app.route('/')
@login_required
def index():
    try:
        # For the index page, we might get missing args, so we pass what we have
        # Pydantic will fill in defaults. For limit, we want a different default (50 vs 20)
        # We can just override it before validation if not present
        req_args = request.args.to_dict()
        if 'limit' not in req_args:
            req_args['limit'] = 50
        args = SearchSchema(**req_args)
    except ValidationError:
        # For UI, if someone messes with the URL, just fallback to defaults instead of a raw JSON error
        args = SearchSchema(limit=50)

    filters = {
        'our_price': args.our_price,
        'our_qty': args.our_qty,
        'min_sup_price': args.min_sup_price,
    }

    status = _get_status()
    # Pass page=1 for initial load
    results = _get_items(args.q.strip(), args.limit, 1, args.sort_by, args.sort_asc, filters)

    return render_template('index.html', 
                           q=args.q, 
                           limit=args.limit, 
                           items=results['items'], 
                           status=status)

@app.route('/ui/history')
@login_required
def ui_history():
    try:
        args = HistorySchema(**request.args.to_dict())
    except ValidationError as e:
        return f"Invalid parameters: {e}", 400
    
    sku = args.sku
    days = args.days
        
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

# --- Health Check ---
@app.route('/health')
@limiter.limit("60 per minute")
def health_check():
    try:
        conn = db.get_connection()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        return {
            "status": "ok", 
            "db": "ok", 
            "version": APP_VERSION
        }, 200
    except Exception as e:
        return {
            "status": "error", 
            "message": str(e), 
            "version": APP_VERSION
        }, 500

# --- Reports ---

@app.route('/reports/spread')
@login_required
def report_spread():
    try:
        req_args = request.args.to_dict()
        if 'exclude' in request.args:
            req_args['exclude'] = request.args.getlist('exclude')
        args = SpreadReportSchema(**req_args)
    except ValidationError as e:
        # Fallback to default if there's invalid values, similar to int() failing gracefully
        args = SpreadReportSchema()
        
    threshold = args.threshold
    per_page = args.limit
    page = args.page
    max_price = args.max_price
    in_stock_only = args.in_stock_only
    exclude_list = args.exclude
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
            except (json.JSONDecodeError, TypeError): continue
        
        query = f"SELECT sku, name, our_price, suppliers_json FROM items_latest WHERE min_sup_price > 0"
        rows = conn.execute(query).fetchall()
        
        results = []
        for r in rows:
            try:
                sups = json.loads(r['suppliers_json'])
            except (json.JSONDecodeError, TypeError): continue
                
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
                'our_price': r['our_price'],
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
    try:
        req_args = request.args.to_dict()
        if 'exclude' in request.args:
            req_args['exclude'] = request.args.getlist('exclude')
        args = MarkupReportSchema(**req_args)
    except ValidationError:
        args = MarkupReportSchema()
    
    markup_pct = args.markup_pct
    per_page = args.limit
    page = args.page
    max_price = args.max_price
    in_stock_only = args.in_stock_only
    qty_equal = args.qty_equal
    exclude_list = args.exclude
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
            except (json.JSONDecodeError, TypeError): continue

        query = "SELECT * FROM items_latest WHERE our_price > 0"
        rows = conn.execute(query).fetchall()
        
        results = []
        for r in rows:
            our = r['our_price']
            our_qty = r['our_qty']
            
            try:
                sups = json.loads(r['suppliers_json'])
            except (json.JSONDecodeError, TypeError): continue
                
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

@app.route('/reports/changes')
@login_required
def report_changes():
    try:
        args = ChangesReportSchema(**request.args.to_dict())
    except ValidationError:
        args = ChangesReportSchema()
        
    days = args.days
    threshold = args.threshold
    type_filter = args.type
    
    conn = db.get_connection()
    try:
        cutoff = int(time.time()) - days * 86400
        
        # Optimize: Fetch only necessary snapshot data first without joining heavy items_latest
        query = """
            SELECT sku, ts, min_sup_price, our_price, min_sup_supplier
            FROM item_snapshots
            WHERE ts >= ?
            ORDER BY sku, ts ASC
        """
        rows = conn.execute(query, (cutoff,)).fetchall()
        
        changes = []
        affected_skus = set()
        
        # Group by SKU
        from itertools import groupby
        from operator import itemgetter
        
        for sku, group in groupby(rows, key=itemgetter('sku')):
            snaps = list(group)
            if len(snaps) < 2:
                continue
            
            # Iterate through snapshots to find changes
            for i in range(1, len(snaps)):
                prev = snaps[i-1]
                curr = snaps[i]
                
                # Check min_sup_price
                if type_filter in ['all', 'min_price']:
                    p_prev = prev['min_sup_price'] or 0
                    p_curr = curr['min_sup_price'] or 0
                    
                    if p_prev > 0 and p_curr > 0:
                        diff_pct = (p_curr - p_prev) / p_prev * 100.0
                        if abs(diff_pct) >= threshold:
                            changes.append({
                                'sku': sku,
                                'ts': curr['ts'],
                                'date': datetime.fromtimestamp(curr['ts']).strftime('%Y-%m-%d %H:%M'),
                                'old_price': p_prev,
                                'new_price': p_curr,
                                'old_supplier': prev['min_sup_supplier'],
                                'new_supplier': curr['min_sup_supplier'],
                                'diff_pct': round(diff_pct, 1),
                                'type': 'min_price', # Market Price
                            })
                            affected_skus.add(sku)

                # Check our_price
                if type_filter in ['all', 'our_price']:
                    p_prev_our = prev['our_price'] or 0
                    p_curr_our = curr['our_price'] or 0
                    
                    if p_prev_our > 0 and p_curr_our > 0:
                        diff_pct = (p_curr_our - p_prev_our) / p_prev_our * 100.0
                        if abs(diff_pct) >= threshold:
                            changes.append({
                                'sku': sku,
                                'ts': curr['ts'],
                                'date': datetime.fromtimestamp(curr['ts']).strftime('%Y-%m-%d %H:%M'),
                                'old_price': p_prev_our,
                                'new_price': p_curr_our,
                                'old_supplier': "Наш магазин",
                                'new_supplier': "Наш магазин",
                                'diff_pct': round(diff_pct, 1),
                                'type': 'our_price', # Our Price
                            })
                            affected_skus.add(sku)
        
        # Batch fetch details for affected SKUs
        if affected_skus:
            placeholders = ','.join(['?'] * len(affected_skus))
            details_query = f"SELECT sku, name, suppliers_json, our_price FROM items_latest WHERE sku IN ({placeholders})"
            details_rows = conn.execute(details_query, list(affected_skus)).fetchall()
            details_map = {r['sku']: r for r in details_rows}
            
            # Enrich changes with details
            valid_changes = []
            for c in changes:
                if c['sku'] in details_map:
                    item = details_map[c['sku']]
                    c['name'] = item['name']
                    c['suppliers_json'] = item['suppliers_json']
                    c['current_our_price'] = item['our_price']
                    valid_changes.append(c)
            changes = valid_changes
        else:
            changes = []

        # Sort by latest change first, then largest change
        changes.sort(key=lambda x: (x['ts'], abs(x['diff_pct'])), reverse=True)
        
        return render_template('report_changes.html',
                               items=changes,
                               days=days,
                               threshold=threshold,
                               type=type_filter)
    finally:
        conn.close()

@app.route('/api/reload')
def api_reload():
    auth_header = request.headers.get('Authorization')
    token = None
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split(' ', 1)[1]
    expected_token = os.environ.get("RELOAD_TOKEN", "")
    
    token_len = len(token) if token else 0
    print(f"[API] Reload requested. Received token: {'set' if token else 'missing'} (len={token_len}), Expected len: {len(expected_token)}")
    
    # Allow if token matches OR user is logged in
    if not (expected_token and token == expected_token) and not current_user.is_authenticated:
        if not expected_token:
            return jsonify({"ok": False, "error": "RELOAD_TOKEN not set in /etc/priceweb_new.env"}), 403
        return jsonify({"ok": False, "error": f"Unauthorized (received len {token_len}, expected len {len(expected_token)})"}), 403
    
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
