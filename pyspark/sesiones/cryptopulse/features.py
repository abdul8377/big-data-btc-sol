"""Mismas variables retrospectivas en batch y streaming; sin etiquetas futuras en inferencia."""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from .core import FEATURES

def past_features(df):
    order = Window.partitionBy("symbol").orderBy("open_ms")
    result = df.withColumn("close_d", F.col("close").cast("double"))
    for lag in (1, 5, 15, 30, 60):
        result = result.withColumn(f"_p{lag}", F.lag("close_d", lag).over(order))
        result = result.withColumn(f"_t{lag}", F.lag("open_ms", lag).over(order))
        result = result.withColumn(f"r{lag}", F.when(
            (F.col("open_ms") - F.col(f"_t{lag}") == lag * 60000) & (F.col(f"_p{lag}") > 0),
            100 * (F.col("close_d") / F.col(f"_p{lag}") - 1)))
    result = result.withColumn("log_r1", F.when(
        F.col("r1").isNotNull(), F.log(F.col("close_d") / F.col("_p1"))))
    for n in (15, 60):
        win = order.rowsBetween(-n + 1, 0)
        result = result.withColumn(f"rv{n}", F.when(
            (F.count("log_r1").over(win) == n) & (F.col("open_ms") - F.col(f"_t{n}") == n * 60000),
            100 * F.sqrt(F.sum(F.pow("log_r1", 2)).over(win))))
    for n in (5, 60):
        win = order.rowsBetween(-n + 1, 0)
        complete = (F.count("*").over(win) == n) & (F.col("open_ms") - F.min("open_ms").over(win) == (n-1)*60000)
        result = result.withColumn(f"quote{n}", F.when(complete,
            F.sum(F.col("quote_asset_volume").cast("double")).over(win)))
        result = result.withColumn(f"trades{n}", F.when(complete,
            F.sum("number_of_trades").over(win)))
    hour = F.hour(F.timestamp_millis("close_ms"))
    result = (result.withColumn("hour_utc", hour)
        .withColumn("log_quote5", F.log1p("quote5"))
        .withColumn("log_trades5", F.log1p("trades5"))
        .withColumn("volume_ratio", F.when(F.col("quote60") > 0, 12 * F.col("quote5") / F.col("quote60")))
        .withColumn("range_pct", 100 * (F.col("high").cast("double") - F.col("low").cast("double")) / F.col("open").cast("double"))
        .withColumn("body_pct", 100 * (F.col("close_d") - F.col("open").cast("double")) / F.col("open").cast("double"))
        .withColumn("hour_sin", F.sin(hour * (2 * 3.141592653589793 / 24)))
        .withColumn("hour_cos", F.cos(hour * (2 * 3.141592653589793 / 24)))
        .withColumn("is_sol", F.when(F.col("symbol") == "SOLUSDT", 1.0).otherwise(0.0)))
    return result.drop(*[f"_{kind}{n}" for kind in ("p", "t") for n in (1, 5, 15, 30, 60)])

def supervised(df):
    order = Window.partitionBy("symbol").orderBy("open_ms")
    result = past_features(df)
    for horizon in (5, 15, 30):
        result = result.withColumn(f"_future_t{horizon}", F.lead("open_ms", horizon).over(order))
        result = result.withColumn(f"_future_c{horizon}", F.lead("close_d", horizon).over(order))
        result = result.withColumn(f"future_r{horizon}", F.when(
            F.col(f"_future_t{horizon}") - F.col("open_ms") == horizon * 60000,
            100 * (F.col(f"_future_c{horizon}") / F.col("close_d") - 1)))
    win15 = order.rowsBetween(1, 15)
    win5 = order.rowsBetween(1, 5)
    return (result.withColumn("target_volatility", F.when(
        (F.count("log_r1").over(win15) == 15) & F.col("future_r15").isNotNull(),
        100 * F.sqrt(F.sum(F.pow("log_r1", 2)).over(win15))))
        .withColumn("future_quote5", F.when(F.col("future_r5").isNotNull(),
            F.sum(F.col("quote_asset_volume").cast("double")).over(win5)))
        .withColumn("future_trades5", F.when(F.col("future_r5").isNotNull(),
            F.sum("number_of_trades").over(win5)))
        .withColumn("future_hour5", F.hour(F.timestamp_millis(F.col("close_ms") + 300000)))
        .drop(*[f"_future_{kind}{n}" for kind in ("t", "c") for n in (5, 15, 30)]))

def pair_features(df, with_target=False):
    keep = ["open_ms", "close_ms", "close_d", "r1", *[x for x in FEATURES if x != "r1"]]
    if with_target:
        keep += ["future_r15"]
    btc = df.filter(F.col("symbol") == "BTCUSDT").select(
        "open_ms", *[F.col(k).alias("btc_" + k) for k in keep if k != "open_ms"])
    sol = df.filter(F.col("symbol") == "SOLUSDT").select(
        "open_ms", *[F.col(k).alias("sol_" + k) for k in keep if k != "open_ms"])
    result = btc.join(sol, "open_ms", "inner")
    win = Window.orderBy("open_ms").rowsBetween(-59, 0)
    result = result.withColumn("correlation_60m", F.when(
        (F.count("btc_r1").over(win) == 60) & (F.count("sol_r1").over(win) == 60)
        & (F.col("open_ms") - F.min("open_ms").over(win) == 59 * 60000),
        F.try_divide(F.covar_pop("btc_r1", "sol_r1").over(win), F.stddev_pop("btc_r1").over(win) * F.stddev_pop("sol_r1").over(win))))
    if with_target:
        result = result.withColumn("target_divergence", F.col("sol_future_r15") - F.col("btc_future_r15"))
    return result.withColumn("current_spread15", F.col("sol_r15") - F.col("btc_r15"))

PAIR_FEATURES = ["btc_" + f for f in FEATURES] + ["sol_" + f for f in FEATURES] + ["correlation_60m", "current_spread15"]
