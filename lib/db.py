"""SQLite接続・テーブル定義"""
import sqlite3
import os
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "storch.db")


def get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_tables(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS product_master (
        sku_code TEXT PRIMARY KEY,
        asin TEXT,
        product_name TEXT,
        maker TEXT,
        case_quantity INTEGER DEFAULT 1,
        unit_cost REAL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_date TEXT NOT NULL,
        slip_number TEXT,
        store_code TEXT,
        channel TEXT,
        sku_code TEXT NOT NULL,
        quantity INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_orders_sku_date ON orders(sku_code, order_date);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_dedup ON orders(slip_number, sku_code, store_code);

    CREATE TABLE IF NOT EXISTS inventory_cainz (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date TEXT NOT NULL,
        sku_code TEXT NOT NULL,
        quantity INTEGER NOT NULL DEFAULT 0
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_inv_cainz ON inventory_cainz(snapshot_date, sku_code);

    CREATE TABLE IF NOT EXISTS inventory_rsl (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date TEXT NOT NULL,
        sku_code TEXT NOT NULL,
        quantity INTEGER NOT NULL DEFAULT 0
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_inv_rsl ON inventory_rsl(snapshot_date, sku_code);

    CREATE TABLE IF NOT EXISTS inventory_fba (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date TEXT NOT NULL,
        sku_code TEXT NOT NULL,
        asin TEXT,
        fulfillable_quantity INTEGER NOT NULL DEFAULT 0,
        inbound_shipped_quantity INTEGER NOT NULL DEFAULT 0
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_inv_fba ON inventory_fba(snapshot_date, sku_code);

    CREATE TABLE IF NOT EXISTS amazon_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_date TEXT NOT NULL,
        sku_code TEXT NOT NULL,
        asin TEXT,
        quantity INTEGER NOT NULL DEFAULT 0,
        order_id TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_amz_orders_sku_date ON amazon_orders(sku_code, order_date);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_amz_orders_dedup ON amazon_orders(order_id, sku_code);

    CREATE TABLE IF NOT EXISTS purchase_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_date TEXT NOT NULL,
        maker TEXT NOT NULL,
        sku_code TEXT NOT NULL,
        quantity INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'ordered',
        expected_arrival TEXT,
        arrived_date TEXT,
        notes TEXT
    );

    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """)
    defaults = {
        "fba_transfer_lead_time": "7",
        "flexi_lead_time": "75",
        "flexi_order_cycle": "60",
        "petzpark_lead_time": "14",
        "petzpark_order_cycle": "30",
        "z_value_high": "2.05",
        "z_value_normal": "1.65",
    }
    for k, v in defaults.items():
        conn.execute(
            "INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (k, v)
        )
    conn.commit()


def get_setting(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute(
        "SELECT value FROM settings WHERE key=?", (key,)
    ).fetchone()
    return row[0] if row else default


def set_setting(conn: sqlite3.Connection, key: str, value: str):
    conn.execute(
        "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)", (key, value)
    )
    conn.commit()


def load_product_master(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM product_master", conn)


def get_latest_inventory(conn: sqlite3.Connection) -> pd.DataFrame:
    """全倉庫の最新在庫を統合して返す"""
    cainz = pd.read_sql("""
        SELECT sku_code, SUM(quantity) as cainz_qty
        FROM inventory_cainz
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM inventory_cainz)
        GROUP BY sku_code
    """, conn)

    rsl = pd.read_sql("""
        SELECT sku_code, SUM(quantity) as rsl_qty
        FROM inventory_rsl
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM inventory_rsl)
        GROUP BY sku_code
    """, conn)

    fba = pd.read_sql("""
        SELECT sku_code,
               SUM(fulfillable_quantity) as fba_qty,
               SUM(inbound_shipped_quantity) as fba_inbound_qty
        FROM inventory_fba
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM inventory_fba)
        GROUP BY sku_code
    """, conn)

    master = load_product_master(conn)
    if master.empty:
        return pd.DataFrame()

    result = master[["sku_code", "product_name", "maker"]].copy()
    result = result.merge(cainz, on="sku_code", how="left")
    result = result.merge(rsl, on="sku_code", how="left")
    result = result.merge(fba, on="sku_code", how="left")
    for col in ["cainz_qty", "rsl_qty", "fba_qty", "fba_inbound_qty"]:
        result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0).astype(int)
    result["total_qty"] = result["cainz_qty"] + result["rsl_qty"] + result["fba_qty"]
    return result


def get_pending_purchase_orders(conn: sqlite3.Connection) -> pd.DataFrame:
    """発注残（未入荷の発注）を取得"""
    return pd.read_sql("""
        SELECT sku_code, SUM(quantity) as pending_qty
        FROM purchase_orders
        WHERE status = 'ordered'
        GROUP BY sku_code
    """, conn)


def get_all_orders(conn: sqlite3.Connection) -> pd.DataFrame:
    """全チャネルの受注データを統合して返す"""
    ne = pd.read_sql("SELECT order_date, sku_code, channel, quantity FROM orders", conn)
    amz = pd.read_sql(
        "SELECT order_date, sku_code, 'Amazon' as channel, quantity FROM amazon_orders",
        conn,
    )
    if ne.empty and amz.empty:
        return pd.DataFrame(columns=["order_date", "sku_code", "channel", "quantity"])
    combined = pd.concat([ne, amz], ignore_index=True)
    combined["order_date"] = pd.to_datetime(combined["order_date"])
    return combined
