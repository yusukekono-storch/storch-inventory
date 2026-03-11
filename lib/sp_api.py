"""Amazon SP-APIクライアント"""
import io
import time
import pandas as pd
import streamlit as st


def _get_credentials():
    try:
        creds = {
            "refresh_token": st.secrets["sp_api"]["refresh_token"],
            "lwa_app_id": st.secrets["sp_api"]["client_id"],
            "lwa_client_secret": st.secrets["sp_api"]["client_secret"],
            "marketplace": st.secrets["sp_api"].get("marketplace_id", "A1VC38T7YXB528"),
        }
        return creds
    except Exception:
        return None


def fetch_fba_inventory() -> pd.DataFrame:
    """FBA在庫レポートを取得"""
    creds = _get_credentials()
    if creds is None:
        raise ValueError("SP-API認証情報が設定されていません。Settingsページで設定してください。")

    try:
        from sp_api.api import Reports
        from sp_api.base import Marketplaces

        reports = Reports(
            credentials=creds,
            marketplace=Marketplaces.JP,
        )
        # レポート作成リクエスト
        res = reports.create_report(
            reportType="GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA"
        )
        report_id = res.payload["reportId"]

        # レポート完了を待つ
        for _ in range(30):
            status = reports.get_report(report_id)
            if status.payload["processingStatus"] == "DONE":
                doc_id = status.payload["reportDocumentId"]
                break
            time.sleep(10)
        else:
            raise TimeoutError("レポート生成がタイムアウトしました")

        # レポートダウンロード
        doc = reports.get_report_document(doc_id)
        url = doc.payload["url"]
        import requests
        resp = requests.get(url)
        df = pd.read_csv(io.StringIO(resp.text), sep="\t")
        return df

    except ImportError:
        raise ImportError("python-sp-api がインストールされていません")


def fetch_amazon_orders(days: int = 730) -> pd.DataFrame:
    """Amazon注文レポートを取得"""
    creds = _get_credentials()
    if creds is None:
        raise ValueError("SP-API認証情報が設定されていません。Settingsページで設定してください。")

    try:
        from sp_api.api import Reports
        from sp_api.base import Marketplaces
        from datetime import datetime, timedelta

        reports = Reports(
            credentials=creds,
            marketplace=Marketplaces.JP,
        )
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")

        res = reports.create_report(
            reportType="GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
            dataStartTime=start_date,
        )
        report_id = res.payload["reportId"]

        for _ in range(30):
            status = reports.get_report(report_id)
            if status.payload["processingStatus"] == "DONE":
                doc_id = status.payload["reportDocumentId"]
                break
            time.sleep(10)
        else:
            raise TimeoutError("レポート生成がタイムアウトしました")

        doc = reports.get_report_document(doc_id)
        url = doc.payload["url"]
        import requests
        resp = requests.get(url)
        df = pd.read_csv(io.StringIO(resp.text), sep="\t")
        return df

    except ImportError:
        raise ImportError("python-sp-api がインストールされていません")
