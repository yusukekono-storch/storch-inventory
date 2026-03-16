"""発注シミュレーター生成スクリプト

使い方: python generate.py
data/ 配下のCSVを読み込み、flexi版・Petz Park版の発注シミュレーター(Excel)を
output/ フォルダに出力します。
"""
import re
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# プロジェクトルートをパスに追加
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from lib.etl import (
    load_master_csv,
    load_nextengine_csv,
    load_cainz_csv,
    load_rsl_csv,
    load_fba_csv,
)
from lib.forecast import (
    compute_seasonal_indices_bulk,
    compute_monthly_forecast_flexi,
    compute_monthly_forecast_petzpark,
    compute_safety_stock,
    classify_sku_demand,
    compute_past_12m_sales,
    compute_6m_monthly_avg,
    get_daily_demand_avg,
)
from lib.excel_builder import build_simulator_excel

# ── パラメータ ───────────────────────────
FLEXI_LT = 75       # リードタイム(日)
FLEXI_OC = 60       # 発注サイクル(日)
PP_LT = 14
PP_OC = 30

FLEXI_Z_HIGH = 2.05   # 売れ筋
FLEXI_Z_NORMAL = 1.65  # 通常
PP_Z = 2.05            # Petz Park全品

FLEXI_MIN_SAFETY_DAYS = 14
PP_MIN_SAFETY_DAYS = 7

# アクセサリー判定キーワード
ACCESSORY_KEYWORDS = [
    "belt", "softstop", "light", "led", "holder",
    "bag", "multi", "neon-light", "treat", "poop",
]


def _is_accessory(sku_code: str, product_name: str) -> bool:
    text = (sku_code + " " + product_name).lower()
    return any(kw in text for kw in ACCESSORY_KEYWORDS)


def _pp_category(sku_code: str) -> str:
    sku_lower = sku_code.lower()
    if "_onlysample" in sku_lower:
        return "ｻﾝﾌﾟﾙ"
    if "-big" in sku_lower:
        return "大容量"
    return "通常"


def _get_inventory(sku_code, cainz_inv, rsl_inv, fba_inv):
    """各倉庫の在庫数を取得"""
    cainz = 0
    rsl = 0
    fba = 0
    if cainz_inv is not None:
        row = cainz_inv[cainz_inv["sku_code"] == sku_code]
        if not row.empty:
            cainz = int(row["quantity"].iloc[0])
    if rsl_inv is not None:
        row = rsl_inv[rsl_inv["sku_code"] == sku_code]
        if not row.empty:
            rsl = int(row["quantity"].iloc[0])
    if fba_inv is not None:
        row = fba_inv[fba_inv["sku_code"] == sku_code]
        if not row.empty:
            fba = int(row["quantity"].iloc[0])
    return cainz, rsl, fba


def main():
    print("=== Storch 発注シミュレーター生成 ===")

    data_dir = ROOT / "data"
    output_dir = ROOT / "output"
    output_dir.mkdir(exist_ok=True)

    # ── 1. データ読込み ──────────────────────
    master = load_master_csv(data_dir / "master.csv")
    print(f"商品マスター: {len(master)} SKU読込")

    orders = load_nextengine_csv(data_dir / "nextengine.csv")
    date_min = orders["order_date"].min()
    date_max = orders["order_date"].max()
    print(f"受注データ: {len(orders):,}件読込（{date_min}〜{date_max}）")

    cainz_inv, cainz_file = load_cainz_csv(data_dir / "cainz")
    if cainz_inv is not None:
        print(f"カインズ在庫: {len(cainz_inv)} SKU（ファイル: {cainz_file}）")
    else:
        print("カインズ在庫: 未取込（data/cainz/ にファイルなし）")

    rsl_inv, rsl_file = load_rsl_csv(data_dir / "rsl")
    if rsl_inv is not None:
        print(f"RSL在庫: {len(rsl_inv)} SKU（ファイル: {rsl_file}）")
    else:
        print("RSL在庫: 未取込（data/rsl/ にファイルなし）")

    fba_inv, fba_file = load_fba_csv(data_dir / "fba")
    if fba_inv is not None:
        print(f"FBA在庫: {len(fba_inv)} SKU（ファイル: {fba_file}）")
    else:
        print("FBA在庫: 未取込（data/fba/ にファイルなし）")

    # ── 2. メーカー別に分割 ──────────────────
    flexi_master = master[master["maker"].str.lower() == "flexi"].copy()
    pp_master = master[
        master["maker"].str.lower().isin(["petz park", "petzpark"])
    ].copy()

    today_str = datetime.now().strftime("%Y%m%d")

    # ── 3. flexi ─────────────────────────────
    flexi_skus = flexi_master["sku_code"].tolist()
    seasonal_indices = compute_seasonal_indices_bulk(orders, flexi_skus)
    high_demand = classify_sku_demand(orders, "flexi")
    target_month = datetime.now().month

    flexi_data = []
    for _, row in flexi_master.iterrows():
        sku = row["sku_code"]
        cat = "ｱｸｾｻﾘ" if _is_accessory(sku, row["product_name"]) else "本体"
        cainz, rsl, fba = _get_inventory(sku, cainz_inv, rsl_inv, fba_inv)
        z = FLEXI_Z_HIGH if sku in high_demand else FLEXI_Z_NORMAL
        safety = compute_safety_stock(orders, sku, z, FLEXI_LT)
        daily_avg = get_daily_demand_avg(orders, sku)
        safety = max(safety, daily_avg * FLEXI_MIN_SAFETY_DAYS)
        monthly_fc = compute_monthly_forecast_flexi(
            orders, sku, seasonal_indices, target_month
        )
        flexi_data.append({
            "sku_code": sku,
            "product_name": row["product_name"],
            "category": cat,
            "stock_total": cainz + rsl + fba,
            "stock_cainz": cainz,
            "stock_rsl": rsl,
            "stock_fba": fba,
            "sales_12m": compute_past_12m_sales(orders, sku),
            "avg_6m": compute_6m_monthly_avg(orders, sku),
            "monthly_forecast": monthly_fc,
            "safety_stock": round(safety, 1),
            "case_quantity": max(row["case_quantity"], 1),
            "unit_cost": row["unit_cost"],
        })

    # ソート: 本体→アクセサリー、各内で月間予測需要降順
    flexi_data.sort(key=lambda d: (
        0 if d["category"] == "本体" else 1,
        -d["monthly_forecast"],
    ))

    n_main = sum(1 for d in flexi_data if d["category"] == "本体")
    n_acc = sum(1 for d in flexi_data if d["category"] == "ｱｸｾｻﾘ")

    print(f"--- flexi ---")
    print(f"対象SKU: {len(flexi_data)}（本体{n_main} + アクセサリー{n_acc}）")

    # 季節指数サマリー
    peak_months = [f"{m}月={seasonal_indices[m]:.2f}" for m in range(1, 13) if seasonal_indices[m] >= 1.1]
    low_months = [f"{m}月={seasonal_indices[m]:.2f}" for m in range(1, 13) if seasonal_indices[m] <= 0.9]
    si_summary = []
    if peak_months:
        si_summary.append(f"{peak_months[0]}（ピーク）")
    if low_months:
        si_summary.append(f"{low_months[0]}（閑散）")
    if si_summary:
        print(f"季節指数: {'、'.join(si_summary)}")

    flexi_path = output_dir / f"flexi_発注シミュレーター_{today_str}.xlsx"
    build_simulator_excel(
        flexi_path, flexi_data, "flexi",
        seasonal_indices=seasonal_indices,
        params={"lt": FLEXI_LT, "oc": FLEXI_OC},
    )
    print(f"出力: {flexi_path}")

    # ── 4. Petz Park ─────────────────────────
    pp_data = []
    for _, row in pp_master.iterrows():
        sku = row["sku_code"]
        cat = _pp_category(sku)
        cainz, rsl, fba = _get_inventory(sku, cainz_inv, rsl_inv, fba_inv)
        safety = compute_safety_stock(orders, sku, PP_Z, PP_LT)
        daily_avg = get_daily_demand_avg(orders, sku)
        safety = max(safety, daily_avg * PP_MIN_SAFETY_DAYS)
        monthly_fc = compute_monthly_forecast_petzpark(orders, sku)
        pp_data.append({
            "sku_code": sku,
            "product_name": row["product_name"],
            "category": cat,
            "stock_total": cainz + rsl + fba,
            "stock_cainz": cainz,
            "stock_rsl": rsl,
            "stock_fba": fba,
            "sales_12m": compute_past_12m_sales(orders, sku),
            "avg_6m": compute_6m_monthly_avg(orders, sku),
            "monthly_forecast": monthly_fc,
            "safety_stock": round(safety, 1),
            "case_quantity": max(row["case_quantity"], 1),
            "unit_cost": row["unit_cost"],
        })

    # ソート: 通常→大容量→サンプル、各内で月間予測需要降順
    cat_order = {"通常": 0, "大容量": 1, "ｻﾝﾌﾟﾙ": 2}
    pp_data.sort(key=lambda d: (cat_order.get(d["category"], 9), -d["monthly_forecast"]))

    n_normal = sum(1 for d in pp_data if d["category"] == "通常")
    n_big = sum(1 for d in pp_data if d["category"] == "大容量")
    n_sample = sum(1 for d in pp_data if d["category"] == "ｻﾝﾌﾟﾙ")

    print(f"--- Petz Park ---")
    print(f"対象SKU: {len(pp_data)}（通常{n_normal} + 大容量{n_big} + サンプル{n_sample}）")

    pp_path = output_dir / f"petzpark_発注シミュレーター_{today_str}.xlsx"
    build_simulator_excel(
        pp_path, pp_data, "petzpark",
        params={"lt": PP_LT, "oc": PP_OC},
    )
    print(f"出力: {pp_path}")

    print("完了！Excelファイルを開いてB5セルの発注係数を調整してください。")


if __name__ == "__main__":
    main()
