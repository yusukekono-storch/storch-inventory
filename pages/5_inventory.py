"""在庫一覧"""
import streamlit as st
import pandas as pd
from lib.db import get_connection, init_tables, get_latest_inventory, get_all_orders
from datetime import datetime, timedelta

st.set_page_config(page_title="在庫一覧", page_icon="📋", layout="wide")
st.title("📋 在庫一覧")

conn = get_connection()
init_tables(conn)
inventory = get_latest_inventory(conn)

if inventory.empty:
    st.warning("在庫データがありません。設定ページからCSVをアップロードしてください。")
    st.stop()

# フィルタ
col1, col2 = st.columns(2)
with col1:
    maker_filter = st.multiselect("メーカー", inventory["maker"].unique().tolist())
with col2:
    search = st.text_input("SKU/商品名で検索")

filtered = inventory.copy()
if maker_filter:
    filtered = filtered[filtered["maker"].isin(maker_filter)]
if search:
    mask = (filtered["sku_code"].str.contains(search, case=False, na=False) |
            filtered["product_name"].str.contains(search, case=False, na=False))
    filtered = filtered[mask]

# 在庫回転率の計算
orders_df = get_all_orders(conn)
if not orders_df.empty:
    cutoff = datetime.now() - timedelta(days=90)
    recent = orders_df[orders_df["order_date"] >= cutoff]
    sales_90d = recent.groupby("sku_code")["quantity"].sum().reset_index()
    sales_90d = sales_90d.rename(columns={"quantity": "sales_90d"})
    filtered = filtered.merge(sales_90d, on="sku_code", how="left")
    filtered["sales_90d"] = filtered["sales_90d"].fillna(0).astype(int)
    filtered["turnover_days"] = filtered.apply(
        lambda r: round(r["total_qty"] / (r["sales_90d"] / 90), 1) if r["sales_90d"] > 0 else 999,
        axis=1,
    )
else:
    filtered["sales_90d"] = 0
    filtered["turnover_days"] = 999

col_labels = {
    "sku_code": "SKUコード",
    "product_name": "商品名",
    "maker": "メーカー",
    "cainz_qty": "カインズ",
    "rsl_qty": "RSL",
    "fba_qty": "FBA",
    "total_qty": "合計",
    "sales_90d": "90日販売数",
    "turnover_days": "在庫回転日数",
}

display = filtered.rename(columns=col_labels)
st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "在庫回転日数": st.column_config.NumberColumn(format="%.1f日"),
    },
)
st.caption(f"表示: {len(filtered)}件 / 全{len(inventory)}件")
