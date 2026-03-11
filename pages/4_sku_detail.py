"""SKU詳細・需要予測グラフ"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from lib.db import get_connection, init_tables, load_product_master, get_latest_inventory, get_all_orders, get_setting, get_pending_purchase_orders
from lib import forecast as fc

st.set_page_config(page_title="SKU詳細", page_icon="🔍", layout="wide")
st.title("🔍 SKU詳細")

conn = get_connection()
init_tables(conn)
master = load_product_master(conn)

if master.empty:
    st.warning("商品マスターが未登録です。")
    st.stop()

# SKU選択
sku_options = master["sku_code"].tolist()
sku_names = dict(zip(master["sku_code"], master["product_name"]))
selected = st.selectbox(
    "SKUを選択",
    sku_options,
    format_func=lambda x: f"{x} - {sku_names.get(x, '')}",
)

if not selected:
    st.stop()

sku_info = master[master["sku_code"] == selected].iloc[0]
maker = sku_info["maker"]
orders_df = get_all_orders(conn)
inventory = get_latest_inventory(conn)

st.markdown(f"### {sku_info['product_name']}")
st.markdown(f"**メーカー:** {maker} | **ケース入数:** {sku_info['case_quantity']} | **仕入単価:** ¥{sku_info['unit_cost']:,.1f}")

# 在庫情報
inv_row = inventory[inventory["sku_code"] == selected]
if not inv_row.empty:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("カインズ", int(inv_row["cainz_qty"].iloc[0]))
    c2.metric("RSL", int(inv_row["rsl_qty"].iloc[0]))
    c3.metric("FBA", int(inv_row["fba_qty"].iloc[0]))
    c4.metric("合計", int(inv_row["total_qty"].iloc[0]))

st.markdown("---")

if orders_df.empty:
    st.info("受注データがありません。")
    st.stop()

# パラメータ
if maker.lower() == "flexi":
    lt = int(get_setting(conn, "flexi_lead_time", "75"))
    cycle = int(get_setting(conn, "flexi_order_cycle", "60"))
    z = float(get_setting(conn, "z_value_high", "2.05"))
    min_days = 14
else:
    lt = int(get_setting(conn, "petzpark_lead_time", "14"))
    cycle = int(get_setting(conn, "petzpark_order_cycle", "30"))
    z = float(get_setting(conn, "z_value_high", "2.05"))
    min_days = 7

safety = fc.compute_safety_stock(orders_df, selected, z, lt)
daily_avg = fc.get_daily_demand_avg(orders_df, selected)
safety = max(safety, daily_avg * min_days)

# --- 需要予測グラフ ---
st.subheader("📈 需要予測グラフ")
forecast = fc.get_forecast(orders_df, selected, maker, lt, cycle)

# 過去12ヶ月実績
sku_orders = orders_df[orders_df["sku_code"] == selected].copy()
sku_orders["order_date"] = pd.to_datetime(sku_orders["order_date"])
sku_orders["month"] = sku_orders["order_date"].dt.to_period("M")
monthly_actual = sku_orders.groupby("month")["quantity"].sum().reset_index()
monthly_actual["month_dt"] = monthly_actual["month"].dt.to_timestamp()
cutoff = datetime.now() - timedelta(days=365)
monthly_actual = monthly_actual[monthly_actual["month_dt"] >= cutoff]

fig = go.Figure()
if not monthly_actual.empty:
    fig.add_trace(go.Bar(
        x=monthly_actual["month_dt"],
        y=monthly_actual["quantity"],
        name="実績",
        marker_color="steelblue",
    ))

if not forecast.empty:
    forecast["date"] = pd.to_datetime(forecast["date"])
    forecast["month"] = forecast["date"].dt.to_period("M")
    monthly_fc = forecast.groupby("month")["forecast_qty"].sum().reset_index()
    monthly_fc["month_dt"] = monthly_fc["month"].dt.to_timestamp()
    fig.add_trace(go.Bar(
        x=monthly_fc["month_dt"],
        y=monthly_fc["forecast_qty"],
        name="予測",
        marker_color="coral",
        opacity=0.7,
    ))

fig.update_layout(
    title="月別販売実績と需要予測",
    xaxis_title="月",
    yaxis_title="数量",
    barmode="group",
    height=400,
)
st.plotly_chart(fig, use_container_width=True)

# --- 在庫消化シミュレーション ---
st.subheader("📉 在庫消化シミュレーション")
current_stock = int(inv_row["total_qty"].iloc[0]) if not inv_row.empty else 0
pending = get_pending_purchase_orders(conn)
pend_row = pending[pending["sku_code"] == selected] if not pending.empty else pd.DataFrame()
pending_qty = int(pend_row["pending_qty"].iloc[0]) if not pend_row.empty else 0

if not forecast.empty and current_stock > 0:
    days = len(forecast)
    stock_no_order = [current_stock]
    stock_with_order = [current_stock + pending_qty]
    for i in range(1, days):
        daily_demand = forecast.iloc[i]["forecast_qty"]
        stock_no_order.append(max(0, stock_no_order[-1] - daily_demand))
        stock_with_order.append(max(0, stock_with_order[-1] - daily_demand))

    sim_fig = go.Figure()
    sim_fig.add_trace(go.Scatter(
        x=forecast["date"], y=stock_no_order,
        name="発注なし", line=dict(color="red", dash="dash"),
    ))
    sim_fig.add_trace(go.Scatter(
        x=forecast["date"], y=stock_with_order,
        name="発注残あり", line=dict(color="green"),
    ))
    sim_fig.add_hline(y=safety, line_dash="dot", line_color="orange",
                      annotation_text=f"安全在庫: {safety:.0f}")
    sim_fig.update_layout(title="在庫消化シミュレーション", height=350,
                          xaxis_title="日付", yaxis_title="在庫数")
    st.plotly_chart(sim_fig, use_container_width=True)

# --- パラメータ表示 ---
st.subheader("⚙️ パラメータ")
reorder_point = sum(forecast.head(lt)["forecast_qty"]) + safety if not forecast.empty else 0
target_inv = forecast["forecast_qty"].sum() + safety if not forecast.empty else 0

p1, p2, p3, p4 = st.columns(4)
p1.metric("安全在庫", f"{safety:.0f}")
p2.metric("発注点", f"{reorder_point:.0f}")
p3.metric("目標在庫", f"{target_inv:.0f}")
p4.metric("リードタイム", f"{lt}日")

# --- 月別販売実績テーブル ---
st.subheader("📋 月別販売実績（チャネル別）")
monthly_sales = fc.get_monthly_sales(orders_df, selected)
if not monthly_sales.empty:
    pivot = monthly_sales.pivot_table(index="month", columns="channel", values="quantity", aggfunc="sum", fill_value=0)
    pivot["合計"] = pivot.sum(axis=1)
    st.dataframe(pivot.sort_index(ascending=False), use_container_width=True)
