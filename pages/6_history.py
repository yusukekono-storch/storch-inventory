"""発注履歴・発注残管理"""
import streamlit as st
import pandas as pd
from datetime import datetime
from lib.db import get_connection, init_tables, load_product_master

st.set_page_config(page_title="発注履歴", page_icon="📝", layout="wide")
st.title("📝 発注履歴")

conn = get_connection()
init_tables(conn)
master = load_product_master(conn)

# --- 新規発注登録 ---
st.subheader("➕ 新規発注登録")
with st.form("new_order"):
    col1, col2, col3 = st.columns(3)
    with col1:
        maker = st.selectbox("メーカー", ["flexi", "Petz Park", "その他"])
    with col2:
        sku_options = master["sku_code"].tolist() if not master.empty else []
        sku = st.selectbox("SKU", sku_options)
    with col3:
        qty = st.number_input("発注数量", min_value=1, value=1)

    col4, col5 = st.columns(2)
    with col4:
        order_date = st.date_input("発注日", datetime.now())
    with col5:
        expected = st.date_input("入荷予定日", None)

    notes = st.text_input("備考")
    submitted = st.form_submit_button("発注を記録")

    if submitted and sku:
        conn.execute("""
            INSERT INTO purchase_orders (order_date, maker, sku_code, quantity, status, expected_arrival, notes)
            VALUES (?, ?, ?, ?, 'ordered', ?, ?)
        """, (order_date.strftime("%Y-%m-%d"), maker, sku, qty,
              expected.strftime("%Y-%m-%d") if expected else None, notes))
        conn.commit()
        st.success(f"発注を記録しました: {sku} x {qty}")
        st.rerun()

st.markdown("---")

# --- 発注履歴一覧 ---
st.subheader("📋 発注履歴")
tab_pending, tab_arrived = st.tabs(["未入荷", "入荷済み"])

with tab_pending:
    df = pd.read_sql(
        "SELECT id, order_date, maker, sku_code, quantity, expected_arrival, notes FROM purchase_orders WHERE status='ordered' ORDER BY order_date DESC",
        conn,
    )
    if df.empty:
        st.info("未入荷の発注はありません。")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

        # 入荷消込
        st.subheader("✅ 入荷消込")
        order_id = st.selectbox("消込む発注ID", df["id"].tolist())
        if st.button("入荷済みにする"):
            conn.execute(
                "UPDATE purchase_orders SET status='arrived', arrived_date=? WHERE id=?",
                (datetime.now().strftime("%Y-%m-%d"), order_id),
            )
            conn.commit()
            st.success("入荷済みに更新しました。")
            st.rerun()

with tab_arrived:
    df_arrived = pd.read_sql(
        "SELECT id, order_date, maker, sku_code, quantity, arrived_date, notes FROM purchase_orders WHERE status='arrived' ORDER BY arrived_date DESC LIMIT 100",
        conn,
    )
    if df_arrived.empty:
        st.info("入荷済みの記録はありません。")
    else:
        st.dataframe(df_arrived, use_container_width=True, hide_index=True)
