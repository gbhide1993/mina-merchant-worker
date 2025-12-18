from utils import normalize_phone_for_db
from datetime import datetime, timedelta
from contextlib import contextmanager
import os
import json

# Use DATABASE_URL from environment or default to local SQLite
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    DB_URL = "sqlite:///local_mina.db"
    print("⚠️ No DATABASE_URL found, using local SQLite: local_mina.db")

# Determine database type and import appropriate driver
IS_POSTGRES = DB_URL.startswith('postgresql:') or DB_URL.startswith('postgres:')

# --- SQL DIALECT COMPATIBILITY LAYER ---
if IS_POSTGRES:
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        PSYCOPG_VERSION = 2
    except ImportError:
        import psycopg
        from psycopg.rows import dict_row
        PSYCOPG_VERSION = 3
    
    PK_TYPE = "SERIAL PRIMARY KEY"
    TIMESTAMP_DEFAULT = "DEFAULT NOW()"
    LIKE_OPERATOR = "ILIKE"
else:
    import sqlite3
    PK_TYPE = "INTEGER PRIMARY KEY"
    TIMESTAMP_DEFAULT = "DEFAULT CURRENT_TIMESTAMP"
    LIKE_OPERATOR = "LIKE"

@contextmanager
def get_conn():
    if IS_POSTGRES:
        if PSYCOPG_VERSION == 2:
            conn = psycopg2.connect(DB_URL)
        else:
            conn = psycopg.connect(DB_URL)
    else:
        conn = sqlite3.connect("local_mina.db")
        conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        try: conn.close()
        except: pass

@contextmanager
def get_cursor():
    """Yields a cursor that commits on success and rolls back on failure."""
    with get_conn() as conn:
        if IS_POSTGRES and PSYCOPG_VERSION == 2:
            cur = conn.cursor(cursor_factory=RealDictCursor)
        elif IS_POSTGRES and PSYCOPG_VERSION == 3:
            conn.row_factory = dict_row
            cur = conn.cursor()
        else:
            cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            try: cur.close()
            except: pass

def fetchone_normalized(cur):
    row = cur.fetchone()
    if not row: return None
    if hasattr(row, "items") or isinstance(row, dict): return dict(row)
    if cur.description:
        cols = [d[0] for d in cur.description] 
        return dict(zip(cols, row))
    return row

def fetchall_normalized(cur):
    rows = cur.fetchall()
    if not rows: return []
    if hasattr(rows[0], "items") or isinstance(rows[0], dict): return [dict(r) for r in rows]
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]

# --- SMART QUERY EXECUTOR (THE FIX) ---
def execute_query(cur, sql, params=None):
    """Automatically swaps %s to ? if using SQLite"""
    if params is None:
        params = ()
    
    if not IS_POSTGRES:
        # SQLite uses ? placeholders, Postgres uses %s
        sql = sql.replace("%s", "?")
    
    cur.execute(sql, params)

# ==========================================
# 1. INITIALIZATION
# ==========================================

def init_db():
    with get_cursor() as cur:
        # Users
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS users (
            id {PK_TYPE},
            phone TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP {TIMESTAMP_DEFAULT},
            subscription_tier VARCHAR(20) DEFAULT 'free',
            credits_remaining FLOAT DEFAULT 30.0,
            subscription_active BOOLEAN DEFAULT FALSE,
            subscription_expiry TIMESTAMP,
            razorpay_customer_id TEXT,
            business_name TEXT, 
            gstin TEXT,
            preferred_language TEXT DEFAULT 'hi',
            current_state VARCHAR(100),
            state_metadata TEXT DEFAULT '{{}}'
        );""")

        # Merchant Tables
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS customers_merchant (
            id {PK_TYPE},
            merchant_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            phone TEXT,
            gstin TEXT,
            billing_address TEXT,
            email TEXT,
            current_balance FLOAT DEFAULT 0.0,
            created_at TIMESTAMP {TIMESTAMP_DEFAULT},
            UNIQUE(merchant_id, phone)
        );""")

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS products_merchant (
            id {PK_TYPE},
            merchant_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            alias TEXT,
            description TEXT,
            unit VARCHAR(20) DEFAULT 'pcs',
            price FLOAT DEFAULT 0.0,
            stock_qty FLOAT DEFAULT 0.0,
            hsn_code TEXT,
            gst_rate FLOAT DEFAULT 0.0,
            created_at TIMESTAMP {TIMESTAMP_DEFAULT}
        );""")

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS orders_merchant (
            id {PK_TYPE},
            merchant_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            customer_id INTEGER REFERENCES customers_merchant(id),
            invoice_number TEXT,
            invoice_date TIMESTAMP {TIMESTAMP_DEFAULT},
            due_date TIMESTAMP,
            final_amount FLOAT DEFAULT 0.0,
            status VARCHAR(20) DEFAULT 'draft',
            payment_status VARCHAR(20) DEFAULT 'unpaid',
            pdf_url TEXT,
            notes TEXT,
            created_at TIMESTAMP {TIMESTAMP_DEFAULT}
        );""")

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS order_items_merchant (
            id {PK_TYPE},
            order_id INTEGER NOT NULL REFERENCES orders_merchant(id) ON DELETE CASCADE,
            product_id INTEGER REFERENCES products_merchant(id),
            product_name TEXT NOT NULL,
            quantity FLOAT NOT NULL,
            unit_price FLOAT NOT NULL,
            gst_rate FLOAT DEFAULT 0.0,
            total_price FLOAT NOT NULL
        );""")
        
        # Legacy Tables (Simplified)
        cur.execute(f"CREATE TABLE IF NOT EXISTS meeting_notes (id {PK_TYPE}, phone TEXT, audio_file TEXT, transcript TEXT, summary TEXT, message_sid TEXT, created_at TIMESTAMP {TIMESTAMP_DEFAULT});")
        cur.execute(f"CREATE TABLE IF NOT EXISTS tasks (id {PK_TYPE}, user_id INTEGER, title TEXT, description TEXT, due_at TIMESTAMP, status VARCHAR(20), metadata TEXT, created_at TIMESTAMP {TIMESTAMP_DEFAULT});")

        try:
            # Migrations
            execute_query(cur, "ALTER TABLE users ADD COLUMN IF NOT EXISTS current_state VARCHAR(100);")
        except: pass

        print(f"✅ DB Merchant Initialized ({'Postgres' if IS_POSTGRES else 'SQLite'}).")

# ==========================================
# 2. USER & STATE FUNCTIONS
# ==========================================

def get_or_create_user(raw_phone):
    phone = normalize_phone_for_db(raw_phone)
    with get_cursor() as cur:
        execute_query(cur, "SELECT * FROM users WHERE phone = %s", (phone,))
        user = fetchone_normalized(cur)
        if not user:
            execute_query(cur, "INSERT INTO users (phone) VALUES (%s)", (phone,))
            if IS_POSTGRES:
                user = fetchone_normalized(cur) # RETURNING supported
            else:
                # SQLite needs manual fetch
                execute_query(cur, "SELECT * FROM users WHERE phone = %s", (phone,))
                user = fetchone_normalized(cur)
        return user

def get_user_by_phone(raw_phone):
    phone = normalize_phone_for_db(raw_phone)
    with get_cursor() as cur:
        execute_query(cur, "SELECT * FROM users WHERE phone = %s", (phone,))
        return fetchone_normalized(cur)

def set_user_state(phone, state, metadata=None):
    phone = normalize_phone_for_db(phone)
    meta_str = json.dumps(metadata or {})
    with get_cursor() as cur:
        get_or_create_user(phone)
        execute_query(cur, "UPDATE users SET current_state = %s, state_metadata = %s WHERE phone = %s", (state, meta_str, phone))

def get_user_state(phone):
    phone = normalize_phone_for_db(phone)
    with get_cursor() as cur:
        execute_query(cur, "SELECT current_state, state_metadata FROM users WHERE phone = %s", (phone,))
        row = fetchone_normalized(cur)
        if row:
            try: meta = json.loads(row.get('state_metadata') or '{}')
            except: meta = {}
            return row.get('current_state'), meta
    return None, {}

# ==========================================
# 3. MERCHANT LOGIC
# ==========================================

def get_products_merchant(merchant_phone):
    merchant = get_user_by_phone(merchant_phone)
    if not merchant: return []
    with get_cursor() as cur:
        execute_query(cur, "SELECT * FROM products_merchant WHERE merchant_id = %s", (merchant['id'],))
        return fetchall_normalized(cur)

def create_draft_order_merchant(merchant_phone, customer_name, items_list):
    merchant = get_or_create_user(merchant_phone)
    merchant_id = merchant['id']
    
    with get_cursor() as cur:
        # 1. Customer
        execute_query(cur, f"SELECT id FROM customers_merchant WHERE merchant_id=%s AND name {LIKE_OPERATOR} %s", (merchant_id, f"%{customer_name}%"))
        res = fetchone_normalized(cur)
        if res:
            cust_id = res['id']
        else:
            execute_query(cur, "INSERT INTO customers_merchant (merchant_id, name) VALUES (%s, %s)", (merchant_id, customer_name))
            if not IS_POSTGRES:
                execute_query(cur, f"SELECT id FROM customers_merchant WHERE merchant_id=%s AND name {LIKE_OPERATOR} %s", (merchant_id, f"%{customer_name}%"))
                res = fetchone_normalized(cur)
                cust_id = res['id']
            else:
                 # Postgres logic handles RETURNING via execute, simplified here for hybrid
                 execute_query(cur, f"SELECT id FROM customers_merchant WHERE merchant_id=%s AND name {LIKE_OPERATOR} %s", (merchant_id, f"%{customer_name}%"))
                 cust_id = fetchone_normalized(cur)['id']

        # 2. Header
        execute_query(cur, "INSERT INTO orders_merchant (merchant_id, customer_id, status) VALUES (%s, %s, 'draft')", (merchant_id, cust_id))
        
        # Get Order ID
        if IS_POSTGRES:
            execute_query(cur, "SELECT currval(pg_get_serial_sequence('orders_merchant','id'))")
            order_id = cur.fetchone()[0]
        else:
             order_id = cur.lastrowid

        # 3. Items
        total = 0
        for item in items_list:
            p_name = item.get('product', 'Item')
            qty = float(item.get('qty', 1))
            rate = float(item.get('rate', 0))
            line_total = qty * rate
            total += line_total
            
            execute_query(cur, """
                INSERT INTO order_items_merchant (order_id, product_name, quantity, unit_price, total_price)
                VALUES (%s, %s, %s, %s, %s)
            """, (order_id, p_name, qty, rate, line_total))
            
        # 4. Update Total
        execute_query(cur, "UPDATE orders_merchant SET final_amount=%s WHERE id=%s", (total, order_id))
        
        return order_id

def get_order_details_merchant(order_id):
    with get_cursor() as cur:
        execute_query(cur, """
            SELECT o.*, c.name as customer_name, c.phone as customer_phone, u.business_name, u.phone as merchant_phone
            FROM orders_merchant o
            JOIN customers_merchant c ON o.customer_id = c.id
            JOIN users u ON o.merchant_id = u.id
            WHERE o.id = %s
        """, (order_id,))
        order = fetchone_normalized(cur)
        if not order: return None

        execute_query(cur, "SELECT * FROM order_items_merchant WHERE order_id=%s", (order_id,))
        items = fetchall_normalized(cur)
             
        res = dict(order)
        res['items'] = items
        return res

def save_meeting_notes(phone, audio_file, transcript, summary):
    phone = normalize_phone_for_db(phone)
    with get_cursor() as cur:
        execute_query(cur, "INSERT INTO meeting_notes (phone, audio_file, transcript, summary) VALUES (%s, %s, %s, %s)", (phone, audio_file, transcript, summary))