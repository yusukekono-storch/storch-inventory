"""需要予測エンジン"""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


def compute_seasonal_forecast(orders_df: pd.DataFrame, sku_code: str,
                               forecast_days: int = 135) -> pd.DataFrame:
    """flexi: 季節性加重移動平均法"""
    sku_orders = orders_df[orders_df["sku_code"] == sku_code].copy()
    if sku_orders.empty:
        return pd.DataFrame(columns=["date", "forecast_qty"])

    sku_orders["order_date"] = pd.to_datetime(sku_orders["order_date"])
    daily = sku_orders.groupby("order_date")["quantity"].sum().reset_index()
    daily = daily.rename(columns={"order_date": "date"})

    # 日次データを月次に集計
    daily["month"] = daily["date"].dt.to_period("M")
    monthly = daily.groupby("month")["quantity"].sum().reset_index()
    monthly["month_dt"] = monthly["month"].dt.to_timestamp()
    monthly["month_num"] = monthly["month_dt"].dt.month

    if len(monthly) < 3:
        # データ不足時は単純平均
        avg_daily = daily["quantity"].sum() / max((daily["date"].max() - daily["date"].min()).days, 1)
        today = datetime.now().date()
        dates = [today + timedelta(days=i) for i in range(forecast_days)]
        return pd.DataFrame({"date": dates, "forecast_qty": [avg_daily] * forecast_days})

    # Step 2: 季節指数の算出
    yearly_avg = monthly["quantity"].mean()
    seasonal_idx = monthly.groupby("month_num")["quantity"].mean() / yearly_avg
    # 全12ヶ月分を埋める（データがない月は1.0）
    full_seasonal = pd.Series(1.0, index=range(1, 13))
    for idx in seasonal_idx.index:
        full_seasonal[idx] = seasonal_idx[idx]
    seasonal_idx = full_seasonal

    # Step 3: 季節性除去 + EWM
    monthly["seasonal_factor"] = monthly["month_num"].map(seasonal_idx)
    monthly["deseasonalized"] = monthly["quantity"] / monthly["seasonal_factor"]
    monthly = monthly.sort_values("month_dt")
    monthly["trend"] = monthly["deseasonalized"].ewm(span=6, adjust=False).mean()
    latest_trend = monthly["trend"].iloc[-1]

    # Step 4: 予測生成
    today = datetime.now().date()
    dates = [today + timedelta(days=i) for i in range(forecast_days)]
    forecasts = []
    for d in dates:
        m = d.month
        daily_forecast = (latest_trend * seasonal_idx[m]) / 30.0
        forecasts.append(max(daily_forecast, 0))

    return pd.DataFrame({"date": dates, "forecast_qty": forecasts})


def compute_weighted_average_forecast(orders_df: pd.DataFrame, sku_code: str,
                                       forecast_days: int = 44) -> pd.DataFrame:
    """Petz Park: 単純加重移動平均法"""
    sku_orders = orders_df[orders_df["sku_code"] == sku_code].copy()
    if sku_orders.empty:
        return pd.DataFrame(columns=["date", "forecast_qty"])

    sku_orders["order_date"] = pd.to_datetime(sku_orders["order_date"])
    daily = sku_orders.groupby("order_date")["quantity"].sum().reset_index()
    daily = daily.rename(columns={"order_date": "date"})

    today = datetime.now().date()
    # 直近3ヶ月の月次集計
    m1_start = today - timedelta(days=30)
    m2_start = today - timedelta(days=60)
    m3_start = today - timedelta(days=90)

    m1 = daily[(daily["date"].dt.date >= m1_start) & (daily["date"].dt.date < today)]["quantity"].sum()
    m2 = daily[(daily["date"].dt.date >= m2_start) & (daily["date"].dt.date < m1_start)]["quantity"].sum()
    m3 = daily[(daily["date"].dt.date >= m3_start) & (daily["date"].dt.date < m2_start)]["quantity"].sum()

    data_days = (today - daily["date"].dt.date.min()).days
    if data_days < 90:
        # 3ヶ月未満: 全期間平均
        total = daily["quantity"].sum()
        avg_daily = total / max(data_days, 1)
    else:
        # 加重平均（直近50%, 2ヶ月前30%, 3ヶ月前20%）
        weighted_monthly = m1 * 0.5 + m2 * 0.3 + m3 * 0.2
        avg_daily = weighted_monthly / 30.0

    dates = [today + timedelta(days=i) for i in range(forecast_days)]
    return pd.DataFrame({"date": dates, "forecast_qty": [max(avg_daily, 0)] * forecast_days})


def get_forecast(orders_df: pd.DataFrame, sku_code: str, maker: str,
                 lead_time: int, order_cycle: int) -> pd.DataFrame:
    """メーカーに応じた予測手法を選択"""
    forecast_days = lead_time + order_cycle
    if maker.lower() == "flexi":
        return compute_seasonal_forecast(orders_df, sku_code, forecast_days)
    else:
        return compute_weighted_average_forecast(orders_df, sku_code, forecast_days)


def get_amazon_forecast(orders_df: pd.DataFrame, sku_code: str, maker: str,
                        days: int = 30) -> pd.DataFrame:
    """Amazonチャネルのみの需要予測（FBA補充用）"""
    amz = orders_df[orders_df["channel"] == "Amazon"].copy()
    if maker.lower() == "flexi":
        return compute_seasonal_forecast(amz, sku_code, days)
    else:
        return compute_weighted_average_forecast(amz, sku_code, days)


def compute_safety_stock(orders_df: pd.DataFrame, sku_code: str,
                         z_value: float, lead_time: int) -> float:
    """安全在庫 = Z値 * σ(日次需要) * sqrt(リードタイム)"""
    sku_orders = orders_df[orders_df["sku_code"] == sku_code].copy()
    if sku_orders.empty:
        return 0.0
    sku_orders["order_date"] = pd.to_datetime(sku_orders["order_date"])
    # 日次集計（販売がない日も0として含める）
    date_range = pd.date_range(sku_orders["order_date"].min(), sku_orders["order_date"].max())
    daily = sku_orders.groupby("order_date")["quantity"].sum().reindex(date_range, fill_value=0)
    sigma = daily.std()
    if np.isnan(sigma):
        sigma = 0.0
    return z_value * sigma * np.sqrt(lead_time)


def get_daily_demand_avg(orders_df: pd.DataFrame, sku_code: str, days: int = 180) -> float:
    """直近N日の平均日次需要"""
    sku_orders = orders_df[orders_df["sku_code"] == sku_code].copy()
    if sku_orders.empty:
        return 0.0
    sku_orders["order_date"] = pd.to_datetime(sku_orders["order_date"])
    cutoff = datetime.now() - timedelta(days=days)
    recent = sku_orders[sku_orders["order_date"] >= cutoff]
    if recent.empty:
        return 0.0
    total = recent["quantity"].sum()
    return total / days


def classify_sku_demand(orders_df: pd.DataFrame, maker: str = "flexi") -> set:
    """直近6ヶ月の月平均販売数が上位30%のSKUを返す（売れ筋判定）"""
    cutoff = datetime.now() - timedelta(days=180)
    orders = orders_df.copy()
    orders["order_date"] = pd.to_datetime(orders["order_date"])
    recent = orders[orders["order_date"] >= cutoff]
    if recent.empty:
        return set()
    sku_total = recent.groupby("sku_code")["quantity"].sum()
    threshold = sku_total.quantile(0.7)
    return set(sku_total[sku_total >= threshold].index)


def get_monthly_sales(orders_df: pd.DataFrame, sku_code: str, months: int = 24) -> pd.DataFrame:
    """過去N ヶ月の月別販売実績"""
    sku_orders = orders_df[orders_df["sku_code"] == sku_code].copy()
    if sku_orders.empty:
        return pd.DataFrame(columns=["month", "quantity", "channel"])
    sku_orders["order_date"] = pd.to_datetime(sku_orders["order_date"])
    cutoff = datetime.now() - timedelta(days=months * 30)
    sku_orders = sku_orders[sku_orders["order_date"] >= cutoff]
    sku_orders["month"] = sku_orders["order_date"].dt.to_period("M").astype(str)
    return sku_orders.groupby(["month", "channel"])["quantity"].sum().reset_index()
