"""FBA補充推奨一覧"""
import streamlit as st
import pandas as pd
from lib.db import get_connection, init_tables, load_product_master, get_all_orders
from lib.fba import compute_fba_recommendations

st.set_page_config(page_title="FBA補充", page_icon="🚚", layout="wide")
st.title("🚚 FBA補充推奨一覧")

conn = get_connection()
init_tables(conn)
master = load_product_master(conn)

if master.empty:
    st.warning("商品マスターが未登録です。")
    st.stop()

orders_df = get_all_orders(conn)
recs = compute_fba_recommendations(conn, orders_df)

if recs.empty:
    st.info("FBA補充が必要なSKUはありません。")
    st.stop()

col_labels = {
    "sku_code": "SKUコード",
    "product_name": "商品名",
    "maker": "メーカー",
    "fba_stock": "FBA在庫",
    "fba_inbound": "FBA入荷中",
    "cainz_stock": "カインズ在庫",
    "amz_demand_30d": "Amazon30日予測",
    "fba_safety": "FBA安全在庫",
    "fba_target": "FBA目標在庫",
    "replenish_qty": "補充推奨数",
    "warning": "警告",
}

display = recs.rename(columns=col_labels)
st.dataframe(display, use_container_width=True, hide_index=True)

# 警告があるSKU
warnings = recs[recs["warning"] != ""]
if not warnings.empty:
    st.warning(f"⚠️ 警告: {len(warnings)}件のSKUでカインズ在庫に注意が必要です")

csv = recs.to_csv(index=False)
st.download_button("📥 FBA補充リストCSV出力", csv, "fba_replenish_list.csv", "text/csv")
