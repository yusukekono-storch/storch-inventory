"""CSV/APIデータ取込み・クレンジング"""
import os
import re
import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path

STORE_MAP = {
    "1": "楽天",
    "2": "Yahoo",
    "3": "Shopify",
    "4": "Amazon",
    "8": "ecforce",
}


def import_product_master(conn: sqlite3.Connection, file) -> int:
    df = pd.read_csv(file, encoding="utf-8-sig")
    df = df.rename(columns={
        "SKUコード": "sku_code",
        "ASINコード": "asin",
        "商品名": "product_name",
        "メーカー名": "maker",
        "ケース入数": "case_quantity",
        "仕入単価": "unit_cost",
    })
    required = ["sku_code", "product_name", "maker"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"必須カラム '{col}' がありません")
    df["sku_code"] = df["sku_code"].str.strip()
    df["asin"] = df["asin"].fillna("").astype(str).str.strip()
    df["maker"] = df["maker"].str.strip()
    df["case_quantity"] = pd.to_numeric(df["case_quantity"], errors="coerce").fillna(1).astype(int)
    df["unit_cost"] = pd.to_numeric(df["unit_cost"], errors="coerce").fillna(0)
    count = 0
    for _, row in df.iterrows():
        conn.execute("""
            INSERT OR REPLACE INTO product_master
            (sku_code, asin, product_name, maker, case_quantity, unit_cost)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (row["sku_code"], row["asin"], row["product_name"],
              row["maker"], row["case_quantity"], row["unit_cost"]))
        count += 1
    conn.commit()
    return count


def import_cainz_inventory(conn: sqlite3.Connection, file, filename: str = "") -> int:
    date_match = re.search(r"(\d{8})", filename)
    if date_match:
        snapshot_date = datetime.strptime(date_match.group(1), "%Y%m%d").strftime("%Y-%m-%d")
    else:
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
    df = pd.read_csv(file, encoding="shift_jis")
    if "商品番号" not in df.columns or "【在庫数-引当数】総バラ数" not in df.columns:
        raise ValueError("必須カラム（商品番号, 【在庫数-引当数】総バラ数）がありません")
    df["sku_code"] = df["商品番号"].str.strip()
    df["quantity"] = pd.to_numeric(df["【在庫数-引当数】総バラ数"], errors="coerce").fillna(0).astype(int)
    grouped = df.groupby("sku_code")["quantity"].sum().reset_index()
    conn.execute("DELETE FROM inventory_cainz WHERE snapshot_date=?", (snapshot_date,))
    count = 0
    for _, row in grouped.iterrows():
        conn.execute(
            "INSERT INTO inventory_cainz(snapshot_date, sku_code, quantity) VALUES(?,?,?)",
            (snapshot_date, row["sku_code"], row["quantity"]),
        )
        count += 1
    conn.commit()
    return count


def import_rsl_inventory(conn: sqlite3.Connection, file, filename: str = "") -> int:
    date_match = re.search(r"(\d{8})", filename)
    if date_match:
        snapshot_date = datetime.strptime(date_match.group(1), "%Y%m%d").strftime("%Y-%m-%d")
    else:
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
    df = pd.read_csv(file, encoding="shift_jis", skiprows=3)
    if "店舗内商品コード" not in df.columns or "販売可能在庫数" not in df.columns:
        raise ValueError("必須カラム（店舗内商品コード, 販売可能在庫数）がありません")
    df["sku_code"] = df["店舗内商品コード"].astype(str).str.strip()
    df["quantity"] = pd.to_numeric(
        df["販売可能在庫数"].astype(str).str.strip(), errors="coerce"
    ).fillna(0).astype(int)
    df = df[df["sku_code"] != ""]
    conn.execute("DELETE FROM inventory_rsl WHERE snapshot_date=?", (snapshot_date,))
    count = 0
    for _, row in df.iterrows():
        conn.execute(
            "INSERT INTO inventory_rsl(snapshot_date, sku_code, quantity) VALUES(?,?,?)",
            (snapshot_date, row["sku_code"], row["quantity"]),
        )
        count += 1
    conn.commit()
    return count


def import_nextengine_orders(conn: sqlite3.Connection, file) -> int:
    df = pd.read_csv(file, encoding="cp932")
    col_map = {
        "受注日": "order_date",
        "伝票番号": "slip_number",
        "店舗コード": "store_code",
        "商品コード": "sku_code",
        "受注数": "quantity",
        "受注キャンセル": "cancel_flag",
    }
    df = df.rename(columns=col_map)
    df = df[df["cancel_flag"] == "有効な受注です。"]
    df["store_code"] = df["store_code"].astype(str).str.strip()
    df = df[df["store_code"] != "3"]
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.strftime("%Y-%m-%d")
    df["sku_code"] = df["sku_code"].astype(str).str.strip()
    df["slip_number"] = df["slip_number"].astype(str).str.strip()
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["channel"] = df["store_code"].map(STORE_MAP).fillna("その他")
    count = 0
    for _, row in df.iterrows():
        try:
            conn.execute("""
                INSERT OR IGNORE INTO orders
                (order_date, slip_number, store_code, channel, sku_code, quantity)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (row["order_date"], row["slip_number"], row["store_code"],
                  row["channel"], row["sku_code"], row["quantity"]))
            count += 1
        except Exception:
            pass
    conn.commit()
    return count


def import_amazon_orders_from_report(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df = df.copy()
    df = df.rename(columns={
        "purchase-date": "order_date",
        "sku": "sku_code",
        "quantity": "quantity",
        "amazon-order-id": "order_id",
        "asin": "asin",
        "order-status": "order_status",
    })
    if "order_status" in df.columns:
        df = df[df["order_status"] != "Cancelled"]
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.tz_convert("Asia/Tokyo").dt.strftime("%Y-%m-%d")
    df["sku_code"] = df["sku_code"].astype(str).str.strip()
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    count = 0
    for _, row in df.iterrows():
        try:
            conn.execute("""
                INSERT OR IGNORE INTO amazon_orders
                (order_date, sku_code, asin, quantity, order_id)
                VALUES (?, ?, ?, ?, ?)
            """, (row["order_date"], row["sku_code"],
                  row.get("asin", ""), row["quantity"], row.get("order_id", "")))
            count += 1
        except Exception:
            pass
    conn.commit()
    return count


def import_fba_inventory_from_report(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    snapshot_date = datetime.now().strftime("%Y-%m-%d")
    df = df.copy()
    df = df.rename(columns={
        "seller-sku": "sku_code",
        "asin": "asin",
        "afn-fulfillable-quantity": "fulfillable_quantity",
        "afn-inbound-shipped-quantity": "inbound_shipped_quantity",
    })
    df["sku_code"] = df["sku_code"].astype(str).str.strip()
    df["fulfillable_quantity"] = pd.to_numeric(
        df["fulfillable_quantity"], errors="coerce"
    ).fillna(0).astype(int)
    df["inbound_shipped_quantity"] = pd.to_numeric(
        df["inbound_shipped_quantity"], errors="coerce"
    ).fillna(0).astype(int)
    conn.execute("DELETE FROM inventory_fba WHERE snapshot_date=?", (snapshot_date,))
    count = 0
    for _, row in df.iterrows():
        conn.execute("""
            INSERT INTO inventory_fba
            (snapshot_date, sku_code, asin, fulfillable_quantity, inbound_shipped_quantity)
            VALUES (?, ?, ?, ?, ?)
        """, (snapshot_date, row["sku_code"], row.get("asin", ""),
              row["fulfillable_quantity"], row["inbound_shipped_quantity"]))
        count += 1
    conn.commit()
    return count


def import_fba_inventory_csv(conn: sqlite3.Connection, file) -> int:
    """セラーセントラルFBA在庫管理レポートCSV（CP932, カンマ区切り）を取り込む
    inventory_snapshotテーブルにwarehouse='fba'として保存

    対応カラム名:
      日本語版: 出品者SKU / Amazon出荷在庫(合計) / Amazon納品数(発送済み)
      英語版:   sku / afn-fulfillable-quantity / afn-inbound-shipped-quantity
    """
    snapshot_date = datetime.now().strftime("%Y-%m-%d")

    # 複数のエンコーディング・区切り文字に対応
    for enc, sep in [("cp932", ","), ("utf-8", "\t"), ("utf-8", ",")]:
        try:
            if hasattr(file, "seek"):
                file.seek(0)
            df = pd.read_csv(file, sep=sep, encoding=enc)
            if len(df.columns) > 3:
                break
        except Exception:
            continue
    else:
        raise ValueError("ファイル形式を認識できません")

    # カラム名の正規化（日本語→英語キー）
    col_map = {
        "出品者SKU": "sku",
        "Amazon出荷在庫(合計)": "afn-fulfillable-quantity",
        "Amazon納品数(発送済み)": "afn-inbound-shipped-quantity",
    }
    df = df.rename(columns=col_map)

    if "sku" not in df.columns:
        raise ValueError("必須カラム '出品者SKU' または 'sku' がありません")

    df["sku_code"] = df["sku"].astype(str).str.strip()
    df["quantity"] = pd.to_numeric(
        df.get("afn-fulfillable-quantity", 0), errors="coerce"
    ).fillna(0).astype(int)
    df["inbound_quantity"] = pd.to_numeric(
        df.get("afn-inbound-shipped-quantity", 0), errors="coerce"
    ).fillna(0).astype(int)
    df = df[df["sku_code"] != ""]

    conn.execute(
        "DELETE FROM inventory_snapshot WHERE snapshot_date=? AND warehouse='fba'",
        (snapshot_date,),
    )
    count = 0
    for _, row in df.iterrows():
        conn.execute(
            """INSERT INTO inventory_snapshot
               (snapshot_date, warehouse, sku_code, quantity, inbound_quantity)
               VALUES (?, 'fba', ?, ?, ?)""",
            (snapshot_date, row["sku_code"], row["quantity"], row["inbound_quantity"]),
        )
        count += 1
    conn.commit()
    return count


# ============================================================
# スタンドアロン CSV 読込み関数（generate.py 用、DB不要）
# ============================================================

def _find_latest_file(folder: Path, extensions=(".csv", ".txt")):
    """フォルダ内の最新ファイルを更新日時で自動検出"""
    if not folder.exists():
        return None
    files = [f for f in folder.iterdir()
             if f.is_file() and f.suffix.lower() in extensions]
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


def load_master_csv(filepath: Path) -> pd.DataFrame:
    """商品マスターCSV → DataFrame"""
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    df = df.rename(columns={
        "SKUコード": "sku_code",
        "ASINコード": "asin",
        "商品名": "product_name",
        "メーカー名": "maker",
        "ケース入数": "case_quantity",
        "仕入単価": "unit_cost",
    })
    df["sku_code"] = df["sku_code"].str.strip()
    df["asin"] = df["asin"].fillna("").astype(str).str.strip()
    df["maker"] = df["maker"].str.strip()
    df["case_quantity"] = pd.to_numeric(
        df["case_quantity"], errors="coerce"
    ).fillna(1).astype(int)
    df["unit_cost"] = pd.to_numeric(
        df["unit_cost"], errors="coerce"
    ).fillna(0)
    return df


def load_nextengine_csv(filepath: Path) -> pd.DataFrame:
    """NextEngine受注CSV → DataFrame (フィルタ済み)"""
    df = pd.read_csv(filepath, encoding="cp932")
    col_map = {
        "受注日": "order_date",
        "伝票番号": "slip_number",
        "店舗コード": "store_code",
        "商品コード": "sku_code",
        "受注数": "quantity",
        "受注キャンセル": "cancel_flag",
    }
    df = df.rename(columns=col_map)
    df = df[df["cancel_flag"] == "有効な受注です。"]
    df["store_code"] = df["store_code"].astype(str).str.strip()
    df = df[df["store_code"] != "3"]
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.strftime("%Y-%m-%d")
    df["sku_code"] = df["sku_code"].astype(str).str.strip()
    df["quantity"] = pd.to_numeric(
        df["quantity"], errors="coerce"
    ).fillna(0).astype(int)
    df["channel"] = df["store_code"].map(STORE_MAP).fillna("その他")
    return df[["order_date", "sku_code", "quantity", "store_code", "channel"]]


def load_cainz_csv(folder: Path):
    """カインズ在庫CSV → (DataFrame, ファイル名) or (None, None)"""
    fpath = _find_latest_file(folder)
    if fpath is None:
        return None, None
    df = pd.read_csv(fpath, encoding="shift_jis")
    if "商品番号" not in df.columns or "【在庫数-引当数】総バラ数" not in df.columns:
        return None, None
    df["sku_code"] = df["商品番号"].str.strip()
    df["quantity"] = pd.to_numeric(
        df["【在庫数-引当数】総バラ数"], errors="coerce"
    ).fillna(0).astype(int)
    grouped = df.groupby("sku_code")["quantity"].sum().reset_index()
    return grouped, fpath.name


def load_rsl_csv(folder: Path):
    """RSL在庫CSV → (DataFrame, ファイル名) or (None, None)"""
    fpath = _find_latest_file(folder)
    if fpath is None:
        return None, None
    df = pd.read_csv(fpath, encoding="shift_jis", skiprows=3)
    if "店舗内商品コード" not in df.columns or "販売可能在庫数" not in df.columns:
        return None, None
    df["sku_code"] = df["店舗内商品コード"].astype(str).str.strip()
    df["quantity"] = pd.to_numeric(
        df["販売可能在庫数"].astype(str).str.strip(), errors="coerce"
    ).fillna(0).astype(int)
    df = df[df["sku_code"] != ""]
    return df[["sku_code", "quantity"]], fpath.name


def load_fba_csv(folder: Path):
    """FBA在庫CSV/TSV → (DataFrame, ファイル名) or (None, None)"""
    fpath = _find_latest_file(folder, extensions=(".csv", ".txt"))
    if fpath is None:
        return None, None
    df = None
    for enc, sep in [("utf-8", "\t"), ("cp932", ","), ("cp932", "\t"), ("utf-8", ",")]:
        try:
            df = pd.read_csv(fpath, sep=sep, encoding=enc)
            if len(df.columns) > 2:
                break
        except Exception:
            continue
    if df is None:
        return None, None
    col_map = {
        "出品者SKU": "sku",
        "seller-sku": "sku",
        "Amazon出荷在庫(合計)": "afn-fulfillable-quantity",
        "Amazon納品数(発送済み)": "afn-inbound-shipped-quantity",
    }
    df = df.rename(columns=col_map)
    if "sku" not in df.columns:
        return None, None
    df["sku_code"] = df["sku"].astype(str).str.strip()
    df["quantity"] = pd.to_numeric(
        df.get("afn-fulfillable-quantity", 0), errors="coerce"
    ).fillna(0).astype(int)
    df["inbound"] = pd.to_numeric(
        df.get("afn-inbound-shipped-quantity", 0), errors="coerce"
    ).fillna(0).astype(int)
    df = df[df["sku_code"] != ""]
    return df[["sku_code", "quantity", "inbound"]], fpath.name
