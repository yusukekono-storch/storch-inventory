"""FBA補充ロジック"""
import numpy as np
import pandas as pd
from lib import db, forecast as fc


def compute_fba_recommendations(conn, orders_df: pd.DataFrame) -> pd.DataFrame:
    master = db.load_product_master(conn)
    if master.empty:
        return pd.DataFrame()

    inventory = db.get_latest_inventory(conn)
    fba_lt = int(db.get_setting(conn, "fba_transfer_lead_time", "7"))
    z_high = float(db.get_setting(conn, "z_value_high", "2.05"))

    results = []
    for _, sku in master.iterrows():
        sku_code = sku["sku_code"]
        maker = sku["maker"]

        # Amazon需要予測（30日分）
        amz_forecast = fc.get_amazon_forecast(orders_df, sku_code, maker, days=30)
        if amz_forecast.empty:
            continue
        amz_demand_30d = amz_forecast["forecast_qty"].sum()
        if amz_demand_30d <= 0:
            continue

        # FBA安全在庫
        amz_orders = orders_df[orders_df["channel"] == "Amazon"]
        amz_safety = fc.compute_safety_stock(amz_orders, sku_code, z_high, fba_lt)

        # FBA目標在庫
        fba_target = amz_demand_30d + amz_safety

        # FBA現在庫
        inv_row = inventory[inventory["sku_code"] == sku_code]
        fba_stock = int(inv_row["fba_qty"].iloc[0]) if not inv_row.empty else 0
        fba_inbound = int(inv_row["fba_inbound_qty"].iloc[0]) if not inv_row.empty else 0
        cainz_stock = int(inv_row["cainz_qty"].iloc[0]) if not inv_row.empty else 0

        # FBA補充推奨数
        replenish = fba_target - fba_stock - fba_inbound
        replenish = max(0, round(replenish))

        if replenish <= 0:
            continue

        # カインズ最低維持在庫（楽天+Yahoo+ecforce 14日分 + 安全在庫）
        non_amz_orders = orders_df[orders_df["channel"].isin(["楽天", "Yahoo", "ecforce"])]
        non_amz_daily = fc.get_daily_demand_avg(non_amz_orders, sku_code)
        non_amz_lt = int(db.get_setting(conn, "flexi_lead_time", "75")) if maker.lower() == "flexi" else 14
        non_amz_safety = fc.compute_safety_stock(non_amz_orders, sku_code, 1.65, non_amz_lt)
        cainz_min_keep = non_amz_daily * 14 + non_amz_safety

        warning = ""
        if cainz_stock < replenish:
            warning = "カインズ在庫不足"
        elif cainz_stock - replenish < cainz_min_keep:
            warning = "カインズ最低維持在庫を下回る可能性"

        results.append({
            "sku_code": sku_code,
            "product_name": sku["product_name"],
            "maker": maker,
            "fba_stock": fba_stock,
            "fba_inbound": fba_inbound,
            "cainz_stock": cainz_stock,
            "amz_demand_30d": round(amz_demand_30d, 1),
            "fba_safety": round(amz_safety, 1),
            "fba_target": round(fba_target, 1),
            "replenish_qty": replenish,
            "warning": warning,
        })

    return pd.DataFrame(results)
