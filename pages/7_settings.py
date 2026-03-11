"""設定 - データ管理・CSVアップロード・SP-API"""
import streamlit as st
import pandas as pd
from lib.db import get_connection, init_tables, load_product_master, get_setting, set_setting
from lib import etl, sp_api

st.set_page_config(page_title="設定", page_icon="⚙️", layout="wide")
st.title("⚙️ 設定・データ管理")

conn = get_connection()
init_tables(conn)

# --- CSVアップロード ---
st.subheader("📂 CSVアップロード")

tab_master, tab_cainz, tab_rsl, tab_ne = st.tabs([
    "商品マスター", "カインズ在庫", "RSL在庫", "NextEngine受注"
])

with tab_master:
    st.markdown("商品マスターCSV（UTF-8 BOM）をアップロード")
    file = st.file_uploader("商品マスター", type=["csv"], key="master_upload")
    if file:
        try:
            count = etl.import_product_master(conn, file)
            st.success(f"✅ {count}件のSKUをインポートしました")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"エラー: {e}")

with tab_cainz:
    st.markdown("カインズ商配 在庫CSV（Shift_JIS）をアップロード")
    file = st.file_uploader("カインズ在庫CSV", type=["csv"], key="cainz_upload")
    if file:
        try:
            count = etl.import_cainz_inventory(conn, file, file.name)
            st.success(f"✅ {count}件の在庫データをインポートしました")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"エラー: {e}")

with tab_rsl:
    st.markdown("RSL 在庫エイジングレポートCSV（Shift_JIS）をアップロード")
    file = st.file_uploader("RSL在庫CSV", type=["csv"], key="rsl_upload")
    if file:
        try:
            count = etl.import_rsl_inventory(conn, file, file.name)
            st.success(f"✅ {count}件の在庫データをインポートしました")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"エラー: {e}")

with tab_ne:
    st.markdown("NextEngine 受注データCSV（CP932）をアップロード")
    file = st.file_uploader("NextEngine受注CSV", type=["csv"], key="ne_upload")
    if file:
        try:
            count = etl.import_nextengine_orders(conn, file)
            st.success(f"✅ {count}件の受注データをインポートしました")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"エラー: {e}")

st.markdown("---")

# --- SP-APIデータ取得 ---
st.subheader("🔗 Amazon SP-API")
col1, col2 = st.columns(2)
with col1:
    if st.button("📦 FBA在庫データ取得"):
        try:
            with st.spinner("FBA在庫データを取得中..."):
                df = sp_api.fetch_fba_inventory()
                count = etl.import_fba_inventory_from_report(conn, df)
                st.success(f"✅ {count}件のFBA在庫データを取得しました")
                st.cache_data.clear()
        except Exception as e:
            st.error(f"エラー: {e}")

with col2:
    if st.button("🛍️ Amazon注文データ取得"):
        try:
            with st.spinner("Amazon注文データを取得中..."):
                df = sp_api.fetch_amazon_orders()
                count = etl.import_amazon_orders_from_report(conn, df)
                st.success(f"✅ {count}件のAmazon注文データを取得しました")
                st.cache_data.clear()
        except Exception as e:
            st.error(f"エラー: {e}")

st.markdown("---")

# --- パラメータ設定 ---
st.subheader("⚙️ パラメータ設定")
with st.form("settings_form"):
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**flexi**")
        flexi_lt = st.number_input("リードタイム（日）", value=int(get_setting(conn, "flexi_lead_time", "75")), key="f_lt")
        flexi_cycle = st.number_input("発注サイクル（日）", value=int(get_setting(conn, "flexi_order_cycle", "60")), key="f_cycle")
    with col2:
        st.markdown("**Petz Park**")
        pp_lt = st.number_input("リードタイム（日）", value=int(get_setting(conn, "petzpark_lead_time", "14")), key="pp_lt")
        pp_cycle = st.number_input("発注サイクル（日）", value=int(get_setting(conn, "petzpark_order_cycle", "30")), key="pp_cycle")

    st.markdown("**共通**")
    col3, col4, col5 = st.columns(3)
    with col3:
        z_high = st.number_input("Z値（売れ筋/PP）", value=float(get_setting(conn, "z_value_high", "2.05")), step=0.05, key="z_h")
    with col4:
        z_normal = st.number_input("Z値（通常）", value=float(get_setting(conn, "z_value_normal", "1.65")), step=0.05, key="z_n")
    with col5:
        fba_lt = st.number_input("FBA移動LT（日）", value=int(get_setting(conn, "fba_transfer_lead_time", "7")), key="fba_lt")

    if st.form_submit_button("設定を保存"):
        set_setting(conn, "flexi_lead_time", str(flexi_lt))
        set_setting(conn, "flexi_order_cycle", str(flexi_cycle))
        set_setting(conn, "petzpark_lead_time", str(pp_lt))
        set_setting(conn, "petzpark_order_cycle", str(pp_cycle))
        set_setting(conn, "z_value_high", str(z_high))
        set_setting(conn, "z_value_normal", str(z_normal))
        set_setting(conn, "fba_transfer_lead_time", str(fba_lt))
        st.success("✅ 設定を保存しました")

st.markdown("---")

# --- 商品マスター閲覧・編集 ---
st.subheader("📋 商品マスター")
master = load_product_master(conn)
if not master.empty:
    edited = st.data_editor(master, use_container_width=True, num_rows="dynamic", key="master_editor")
    if st.button("マスターを保存"):
        for _, row in edited.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO product_master
                (sku_code, asin, product_name, maker, case_quantity, unit_cost)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (row["sku_code"], row["asin"], row["product_name"],
                  row["maker"], row["case_quantity"], row["unit_cost"]))
        conn.commit()
        st.success("✅ マスターを保存しました")
        st.cache_data.clear()

# --- DB情報 ---
st.markdown("---")
st.subheader("💾 データベース情報")
tables = ["product_master", "orders", "amazon_orders", "inventory_cainz", "inventory_rsl", "inventory_fba", "purchase_orders"]
for t in tables:
    count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    st.text(f"{t}: {count}件")
