"""Amazon SP-APIクライアント（requestsベース直接実装）"""
import io
import time
import json
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta

LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
SP_API_BASE = "https://sellingpartnerapi-fe.amazon.com"


def _get_credentials():
    try:
        return {
            "refresh_token": st.secrets["sp_api"]["refresh_token"],
            "client_id": st.secrets["sp_api"]["client_id"],
            "client_secret": st.secrets["sp_api"]["client_secret"],
            "marketplace_id": st.secrets["sp_api"].get("marketplace_id", "A1VC38T7YXB528"),
        }
    except Exception:
        return None


def _get_access_token(creds: dict) -> str:
    """LWA (Login with Amazon) でアクセストークンを取得"""
    resp = requests.post(LWA_TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": creds["refresh_token"],
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def _sp_api_headers(access_token: str) -> dict:
    return {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
    }


def _create_report(access_token: str, report_type: str,
                    data_start_time: str = None) -> str:
    """レポート作成をリクエストし、reportId を返す"""
    body = {"reportType": report_type,
            "marketplaceIds": ["A1VC38T7YXB528"]}
    if data_start_time:
        body["dataStartTime"] = data_start_time

    resp = requests.post(
        f"{SP_API_BASE}/reports/2021-06-30/reports",
        headers=_sp_api_headers(access_token),
        json=body,
    )
    resp.raise_for_status()
    return resp.json()["reportId"]


def _poll_report(access_token: str, report_id: str,
                 max_wait: int = 300, interval: int = 10) -> str:
    """レポート完了を待ち、reportDocumentId を返す"""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        resp = requests.get(
            f"{SP_API_BASE}/reports/2021-06-30/reports/{report_id}",
            headers=_sp_api_headers(access_token),
        )
        resp.raise_for_status()
        data = resp.json()
        status = data.get("processingStatus")
        if status == "DONE":
            return data["reportDocumentId"]
        if status in ("CANCELLED", "FATAL"):
            raise RuntimeError(f"レポート生成に失敗しました: {status}")
        time.sleep(interval)
    raise TimeoutError("レポート生成がタイムアウトしました")


def _download_report(access_token: str, document_id: str) -> pd.DataFrame:
    """レポートドキュメントをダウンロードしDataFrameで返す"""
    resp = requests.get(
        f"{SP_API_BASE}/reports/2021-06-30/documents/{document_id}",
        headers=_sp_api_headers(access_token),
    )
    resp.raise_for_status()
    doc = resp.json()
    url = doc["url"]

    dl = requests.get(url)
    dl.raise_for_status()
    return pd.read_csv(io.StringIO(dl.text), sep="	")


def fetch_fba_inventory() -> pd.DataFrame:
    """FBA在庫レポートを取得"""
    creds = _get_credentials()
    if creds is None:
        raise ValueError(
            "SP-API認証情報が設定されていません。Settingsページで設定してください。")

    token = _get_access_token(creds)
    report_id = _create_report(
        token, "GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA")
    doc_id = _poll_report(token, report_id)
    return _download_report(token, doc_id)


def fetch_amazon_orders(days: int = 730) -> pd.DataFrame:
    """Amazon注文レポートを取得"""
    creds = _get_credentials()
    if creds is None:
        raise ValueError(
            "SP-API認証情報が設定されていません。Settingsページで設定してください。")

    token = _get_access_token(creds)
    start = (datetime.utcnow() - timedelta(days=days)).strftime(
        "%Y-%m-%dT00:00:00Z")
    report_id = _create_report(
        token,
        "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
        data_start_time=start,
    )
    doc_id = _poll_report(token, report_id)
    return _download_report(token, doc_id)
