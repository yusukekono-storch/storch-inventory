"""ダッシュボード - KPIカード・アラート"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from lib.db import get_connection, init_tables, load_product_master, get_latest_inventory, get_all_orders, get_setting
from lib.ordering import compute_order_recommendations
from lib.fba import compute_fba_recommendations

st.set_page_config(page_title="Dashboard", page_icon="📊", layout="wide")
st.title("📊 ダッシュボード")

conn = get_connection()
init_tables(conn)
master = load_product_master(conn)

if master.empty:
    st.warning("商品マスターが未登録です。設定ページからCSVをアップロードしてください。")
    st.stop()

orders_df = get_all_orders(conn)
inventory = get_latest_inventory(conn)

# --- KPIカード ---
col1, col2, col3, col4 = st.columns(4)

total_skus = len(master)
total_stock = int(inventory["total_qty"].sum()) if not inventory.empty else 0

with col1:
    st.metric("登録SKU数", f"{total_skus:,}")
with col2:
    st.metric("総在庫数", f"{total_stock:,}")

# 直近30日売上
if not orders_df.empty:
    cutoff = datetime.now() - timedelta(days=30)
    recent_sales = orders_df[orders_df["order_date"] >= cutoff]["quantity"].sum()
    # 前年同月比
    last_year_start = cutoff - timedelta(days=365)
    last_year_end = datetime.now() - timedelta(days=365)
    ly_sales = orders_df[
        (orders_df["order_date"] >= last_year_start) &
        (orders_df["order_date"] < last_year_end)
    ]["quantity"].sum()
    yoy = f"{(recent_sales / ly_sales - 1) * 100:+.1f}%" if ly_sales > 0 else "N/A"
    with col3:
        st.metric("直近30日売上", f"{int(recent_sales):,}個", yoy)
else:
    with col3:
        st.metric("直近30日売上", "データなし")

# 発注推奨
order_recs = compute_order_recommendations(conn, orders_df)
if not order_recs.empty:
    flexi_alerts = len(order_recs[(order_recs["maker"].str.lower() == "flexi") & (order_recs["order_qty"] > 0)])
    pp_alerts = len(order_recs[(order_recs["maker"].str.lower().isin(["petz park", "petzpark"])) & (order_recs["order_qty"] > 0)])
    with col4:
        st.metric("要発注SKU", f"flexi: {flexi_alerts} / PP: {pp_alerts}")
else:
    with col4:
        st.metric("要発注SKU", "0")

st.markdown("---")

# --- アラートセクション ---
st.subheader("⚠️ アラート")

if not order_recs.empty:
    urgent = order_recs[order_recs["urgency"] == "緊急"]
    if not urgent.empty:
        st.error(f"🚨 緊急発注が必要なSKU: {len(urgent)}件")
        st.dataframe(
            urgent[["sku_code", "product_name", "maker", "current_stock", "order_qty"]],
            use_container_width=True,
            hide_index=True,
        )

    caution = order_recs[order_recs["urgency"] == "注意"]
    if not caution.empty:
        st.warning(f"⚡ 注意が必要なSKU: {len(caution)}件")

    # 在庫過多
    excess = order_recs[order_recs["current_stock"] > order_recs["target_inventory"] * 1.5]
    if not excess.empty:
        st.info(f"📦 在庫過多のSKU: {len(excess)}件")

# 欠品リスク（30日以内に在庫ゼロ）
if not inventory.empty and not orders_df.empty:
    risk_skus = []
    for _, row in inventory.iterrows():
        if row["total_qty"] > 0:
            daily_avg = orders_df[orders_df["sku_code"] == row["sku_code"]]["quantity"].sum()
            if not orders_df.empty:
                days_range = (orders_df["order_date"].max() - orders_df["order_date"].min()).days
                if days_range > 0:
                    daily_avg = daily_avg / days_range
                    days_left = row["total_qty"] / daily_avg if daily_avg > 0 else 999
                    if days_left < 30:
                        risk_skus.append({
                            "SKU": row["sku_code"],
                            "商品名": row["product_name"],
                            "現在庫": row["total_qty"],
                            "残日数": round(days_left, 1),
                        })
    if risk_skus:
        st.warning(f"📉 30日以内に欠品リスクのあるSKU: {len(risk_skus)}件")
        st.dataframe(pd.DataFrame(risk_skus), use_container_width=True, hide_index=True)

# FBA補充アラート
fba_recs = compute_fba_recommendations(conn, orders_df)
if not fba_recs.empty:
    st.info(f"🚚 FBA補充推奨: {len(fba_recs)}件 → FBA補充ページで確認")

# 次回flexi発注予定
flexi_lt = int(get_setting(conn, "flexi_order_cycle", "60"))
st.markdown("---")
st.subheader("📅 次回flexi発注予定")
st.info(f"発注サイクル: {flexi_lt}日ごと（発注履歴ページで前回発注日を確認）")
