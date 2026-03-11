"""Streamlitメインアプリ（エントリポイント）"""
import streamlit as st
from lib.db import get_connection, init_tables

st.set_page_config(
    page_title="シュトルヒ在庫管理システム",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)


def check_password():
    """パスワード認証"""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    try:
        correct_pw = st.secrets["password"]
    except Exception:
        # secrets未設定時は認証スキップ（ローカル開発用）
        st.session_state.authenticated = True
        return True

    st.title("🔐 ログイン")
    password = st.text_input("パスワードを入力してください", type="password")
    if st.button("ログイン"):
        if password == correct_pw:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("パスワードが正しくありません")
    return False


def main():
    if not check_password():
        return

    # DB初期化
    conn = get_connection()
    init_tables(conn)
    st.session_state["db_conn"] = conn

    st.title("📦 シュトルヒ在庫管理システム")
    st.markdown("---")
    st.markdown("""
    ### ようこそ！
    左のサイドバーからページを選択してください。

    | ページ | 機能 |
    |--------|------|
    | **Dashboard** | アラート、KPIカード |
    | **発注推奨** | flexi/PP発注推奨一覧 |
    | **FBA補充** | FBA補充推奨一覧 |
    | **SKU詳細** | 需要予測グラフ |
    | **在庫一覧** | 全倉庫×全SKU |
    | **発注履歴** | 発注記録・入荷消込 |
    | **設定** | CSVアップロード・SP-API |
    """)

    st.info("初回利用時は「設定」ページから商品マスターCSVと各在庫CSV、NextEngine受注データをアップロードしてください。")


if __name__ == "__main__":
    main()
