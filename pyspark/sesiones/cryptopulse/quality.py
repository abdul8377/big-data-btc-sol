"""S03: validacion, cuarentena, deduplicacion y Parquet particionado."""
import hashlib
import json
from pathlib import Path
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DecimalType, StringType
from pyspark.sql.window import Window
from .core import ROOT, CLEAN, get_spark, status, save_json

def load_raw(spark):
    data = ROOT / "s02-fundamentos/data"
    manifest = json.loads((data / "provenance.json").read_text(encoding="utf-8"))
    paths = []
    for entry in manifest["archives"]:
        path = data / entry["csv_relative_path"]
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
        if digest.hexdigest() != entry["sha256_csv"]:
            raise ValueError(f"Hash CSV no coincide: {path}")
        paths.append(str(path))
    schema = StructType([StructField("open_time_raw", LongType())] +
        [StructField(c, DecimalType(28, 8)) for c in ["open", "high", "low", "close", "volume"]] +
        [StructField("close_time_raw", LongType()), StructField("quote_asset_volume", DecimalType(28, 8)),
         StructField("number_of_trades", LongType()), StructField("taker_buy_base_volume", DecimalType(28, 8)),
         StructField("taker_buy_quote_volume", DecimalType(28, 8)), StructField("ignore", StringType())])
    raw = (spark.read.schema(schema).option("mode", "FAILFAST").csv(paths)
        .withColumn("source_file", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
        .withColumn("symbol", F.regexp_extract("source_file", r"^(BTCUSDT|SOLUSDT)-", 1))
        .withColumn("archive_month", F.regexp_extract("source_file", r"-(\d{4}-\d{2})\.csv$", 1)))
    raw = (raw.withColumn("open_ms", F.when(F.col("archive_month") >= "2025-01",
                F.expr("open_time_raw DIV 1000")).otherwise(F.col("open_time_raw")))
        .withColumn("close_ms", F.when(F.col("archive_month") >= "2025-01",
                F.expr("close_time_raw DIV 1000")).otherwise(F.col("close_time_raw")))
        .withColumn("event_time", F.timestamp_millis("close_ms")))
    return raw, manifest

def mark_quality(df):
    required = ["symbol", "open_ms", "close_ms", "open", "high", "low", "close", "volume", "quote_asset_volume", "number_of_trades"]
    null = F.lit(False)
    for col in required:
        null = null | F.col(col).isNull()
    errors = F.array_compact(F.array(
        F.when(null, "required_null"),
        F.when(~F.col("symbol").isin("BTCUSDT", "SOLUSDT"), "unknown_symbol"),
        F.when((F.least("open", "high", "low", "close") <= 0)
            | (F.col("high") < F.greatest("open", "close"))
            | (F.col("low") > F.least("open", "close")) | (F.col("high") < F.col("low")), "invalid_ohlc"),
        F.when((F.col("volume") < 0) | (F.col("quote_asset_volume") < 0) | (F.col("number_of_trades") < 0), "negative_activity"),
        F.when((F.col("open_ms") % 60000 != 0) | (F.col("close_ms") - F.col("open_ms") != 59999), "invalid_interval")
    ))
    return df.withColumn("quality_errors", errors)

def run():
    spark = get_spark("s03-calidad")
    out = ROOT / "s03-calidad/artifacts"
    out.mkdir(parents=True, exist_ok=True)
    raw, manifest = load_raw(spark)
    marked = mark_quality(raw).persist(StorageLevel.DISK_ONLY)
    profile = marked.agg(F.count("*").alias("rows"), F.sum((F.size("quality_errors") > 0).cast("long")).alias("invalid"),
        *[F.sum(F.col(c).isNull().cast("long")).alias("null_" + c) for c in raw.columns]).first().asDict()
    assert profile["rows"] == manifest["rows_total"]
    quarantine = marked.filter(F.size("quality_errors") > 0)
    quarantine.write.mode("overwrite").parquet(str(out / "quarantine"))
    valid = marked.filter(F.size("quality_errors") == 0).drop("quality_errors", "ignore")
    # Los conflictos no se resuelven eligiendo un precio arbitrario.
    conflicts = valid.groupBy("symbol", "open_ms").agg(F.countDistinct(
        F.struct("open", "high", "low", "close", "volume", "quote_asset_volume", "number_of_trades")
        ).alias("versions")).filter("versions > 1")
    conflicting = conflicts.count()
    if conflicting:
        valid.join(conflicts.select("symbol", "open_ms"), ["symbol", "open_ms"], "inner").write.mode("overwrite").parquet(str(out / "conflicting_duplicates"))
        valid = valid.join(conflicts.select("symbol", "open_ms"), ["symbol", "open_ms"], "left_anti")
    clean = valid.dropDuplicates(["symbol", "open_ms"])
    clean = (clean.withColumn("year", F.year("event_time"))
        .withColumn("month", F.month("event_time")).withColumn("date_utc", F.to_date("event_time")))
    clean.repartition(16, "symbol", "year", "month").write.mode("overwrite").partitionBy(
        "symbol", "year", "month").parquet(str(CLEAN))
    readback = spark.read.parquet(str(CLEAN))
    clean_rows = readback.count()
    order = Window.partitionBy("symbol").orderBy("open_ms")
    gap_df = (readback.withColumn("delta_ms", F.col("open_ms") - F.lag("open_ms").over(order))
        .filter(F.col("delta_ms") > 60000)
        .withColumn("missing_minutes", F.col("delta_ms") / 60000 - 1))
    gap_df.write.mode("overwrite").parquet(str(out / "gaps"))
    gap = gap_df.agg(F.count("*").alias("gaps"), F.sum("missing_minutes").alias("missing_minutes")).first().asDict()
    groups = [r.asDict() for r in readback.groupBy("symbol").agg(F.count("*").alias("rows"),
        F.min("open_ms").alias("first_open_ms"), F.max("open_ms").alias("last_open_ms")).collect()]
    query = readback.filter((F.col("symbol") == "BTCUSDT") & (F.col("year") == 2025) & (F.col("month") == 1))
    plan = query._jdf.queryExecution().executedPlan().toString()
    assert "PartitionFilters" in plan and query.count() > 0
    (out / "partition_pruning.txt").write_text(plan, encoding="utf-8")
    report = status("S03", input_rows=profile["rows"], clean_rows=clean_rows, invalid_rows=profile["invalid"],
        conflicting_keys=conflicting, duplicate_or_conflict_rows_removed=profile["rows"] - profile["invalid"] - clean_rows,
        null_profile=profile, gaps=gap, assets=groups, partition_pruning=True,
        partition_keys=["symbol", "year", "month"], month_from=manifest["month_from"], month_to=manifest["month_to"],
        policy="No se imputan precios ni velas ausentes. Conflictos se excluyen y se conservan en cuarentena.")
    save_json(out / "verificacion_s03.json", report)
    marked.unpersist()
    spark.stop()
    return report

if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
