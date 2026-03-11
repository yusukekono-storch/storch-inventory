"""発注推奨一覧"""
import streamlit as st
import pandas as pd
from lib.db import get_connection, init_tables, load_product_master, get_all_orders
from lib.ordering import compute_order_recommendations

st.set_page_config(page_title="発注推奨", page_icon="🛒", layout="wide")
st.title("🛒 発注推奨一覧")

conn = get_connection()
init_tables(conn)
master = load_product_master(conn)

if master.empty:
    st.warning("商品マスターが未登録です。")
    st.stop()

orders_df = get_all_orders(conn)
recs = compute_order_recommendations(conn, orders_df)

if recs.empty:
    st.info("受注データがないため発注推奨を計算できません。設定ページからデータをアップロードしてください。")
    st.stop()

# タブ切替
tab_flexi, tab_pp, tab_other = st.tabs(["flexi", "Petz Park", "その他"])

show_all = st.sidebar.checkbox("発注不要SKUも表示", value=False)

display_cols = [
    "sku_code", "product_name", "current_stock", "pending_qty",
    "monthly_forecast", "safety_stock", "reorder_point",
    "order_qty", "order_cases", "case_quantity", "order_amount", "urgency",
]
col_labels = {
    "sku_code": "SKUコード",
    "product_name": "商品名",
    "current_stock": "現在庫合計",
    "pending_qty": "発注残",
    "monthly_forecast": "月間予測需要",
    "safety_stock": "安全在庫",
    "reorder_point": "発注点",
    "order_qty": "発注推奨数(個)",
    "order_cases": "発注ケース数",
    "case_quantity": "ケース入数",
    "order_amount": "発注金額概算",
    "urgency": "緊急度",
}


def show_table(df, tab_name):
    if not show_all:
        df = df[df["order_qty"] > 0]
    if df.empty:
        st.info(f"{tab_name}: 発注不要です")
        return
    display = df[display_cols].rename(columns=col_labels)
    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "発注金額概算": st.column_config.NumberColumn(format="¥%d"),
            "緊急度": st.column_config.TextColumn(),
        },
    )
    total_amount = df["order_amount"].sum()
    st.metric("発注合計金額", f"¥{total_amount:,.0f}")

    csv = df[display_cols].to_csv(index=False)
    st.download_button(
        f"📥 {tab_name} 発注リストCSV出力",
        csv,
        f"order_list_{tab_name}.csv",
        "text/csv",
    )


with tab_flexi:
    flexi = recs[recs["maker"].str.lower() == "flexi"]
    show_table(flexi, "flexi")

with tab_pp:
    pp = recs[recs["maker"].str.lower().isin(["petz park", "petzpark"])]
    show_table(pp, "PetzPark")

with tab_other:
    others = recs[~recs["maker"].str.lower().isin(["flexi", "petz park", "petzpark"])]
    show_table(others, "その他")
