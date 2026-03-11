"""発注算出ロジック"""
import math
import pandas as pd
from lib import db, forecast as fc


def compute_order_recommendations(conn, orders_df: pd.DataFrame) -> pd.DataFrame:
    master = db.load_product_master(conn)
    if master.empty:
        return pd.DataFrame()

    inventory = db.get_latest_inventory(conn)
    pending = db.get_pending_purchase_orders(conn)
    high_demand_skus = fc.classify_sku_demand(orders_df, "flexi")

    flexi_lt = int(db.get_setting(conn, "flexi_lead_time", "75"))
    flexi_cycle = int(db.get_setting(conn, "flexi_order_cycle", "60"))
    pp_lt = int(db.get_setting(conn, "petzpark_lead_time", "14"))
    pp_cycle = int(db.get_setting(conn, "petzpark_order_cycle", "30"))
    z_high = float(db.get_setting(conn, "z_value_high", "2.05"))
    z_normal = float(db.get_setting(conn, "z_value_normal", "1.65"))

    results = []
    for _, sku in master.iterrows():
        sku_code = sku["sku_code"]
        maker = sku["maker"]
        case_qty = max(sku["case_quantity"], 1)
        unit_cost = sku["unit_cost"]

        if maker.lower() == "flexi":
            lt, cycle = flexi_lt, flexi_cycle
            z = z_high if sku_code in high_demand_skus else z_normal
            min_days = 14
        elif maker.lower() in ("petz park", "petzpark"):
            lt, cycle = pp_lt, pp_cycle
            z = z_high
            min_days = 7
        else:
            lt, cycle = pp_lt, pp_cycle
            z = z_normal
            min_days = 7

        # 需要予測
        fcast = fc.get_forecast(orders_df, sku_code, maker, lt, cycle)
        if fcast.empty:
            continue

        forecast_demand_lt = fcast.head(lt)["forecast_qty"].sum()
        forecast_demand_total = fcast["forecast_qty"].sum()
        monthly_forecast = fcast.head(30)["forecast_qty"].sum()

        # 安全在庫
        safety = fc.compute_safety_stock(orders_df, sku_code, z, lt)
        daily_avg = fc.get_daily_demand_avg(orders_df, sku_code)
        safety = max(safety, daily_avg * min_days)

        # 発注点・目標在庫
        reorder_point = forecast_demand_lt + safety
        target_inventory = forecast_demand_total + safety

        # 現在庫
        inv_row = inventory[inventory["sku_code"] == sku_code]
        current_stock = int(inv_row["total_qty"].iloc[0]) if not inv_row.empty else 0

        # 発注残
        pend_row = pending[pending["sku_code"] == sku_code] if not pending.empty else pd.DataFrame()
        pending_qty = int(pend_row["pending_qty"].iloc[0]) if not pend_row.empty else 0

        # 必要発注数
        raw_order = target_inventory - current_stock - pending_qty
        if raw_order <= 0:
            order_qty = 0
            order_cases = 0
        else:
            order_cases = math.ceil(raw_order / case_qty)
            order_qty = order_cases * case_qty

        # 緊急度
        if current_stock < reorder_point * 0.5:
            urgency = "緊急"
        elif current_stock < reorder_point:
            urgency = "注意"
        else:
            urgency = "通常"

        results.append({
            "sku_code": sku_code,
            "product_name": sku["product_name"],
            "maker": maker,
            "current_stock": current_stock,
            "pending_qty": pending_qty,
            "monthly_forecast": round(monthly_forecast, 1),
            "safety_stock": round(safety, 1),
            "reorder_point": round(reorder_point, 1),
            "target_inventory": round(target_inventory, 1),
            "order_qty": order_qty,
            "order_cases": order_cases,
            "case_quantity": case_qty,
            "order_amount": round(order_qty * unit_cost, 0),
            "urgency": urgency,
        })

    return pd.DataFrame(results)
