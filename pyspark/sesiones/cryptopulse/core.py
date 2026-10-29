"""Configuracion y funciones compartidas del pipeline funcional CryptoPulse."""
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(os.environ.get("CRYPTOPULSE_ROOT", "/opt"))
STATE = ROOT / "_runtime"
STATE.mkdir(exist_ok=True)
CLEAN = ROOT / "s03-calidad/artifacts/clean_parquet"
ML = ROOT / "s04-modelos/artifacts"
SYMBOLS = ("BTCUSDT", "SOLUSDT")
TRAIN_END = 1735689600000  # 2025-01-01 UTC
CALIBRATION_END = 1756684800000  # 2025-09-01 UTC
VALIDATION_END = 1767225600000  # 2026-01-01 UTC
FEATURES = ["r1", "r5", "r15", "r30", "rv15", "rv60", "log_quote5", "log_trades5",
            "range_pct", "body_pct", "volume_ratio", "hour_sin", "hour_cos", "is_sol"]

def now():
    return datetime.now(timezone.utc).isoformat()

def save_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Windows bind mounts can briefly lock a destination while a reader opens it.
    # A unique temporary also prevents concurrent writers from sharing a .tmp file.
    descriptor, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(json.dumps(value, indent=2, ensure_ascii=False, default=str) + "\n")
            output.flush()
            os.fsync(output.fileno())
        for attempt in range(7):
            try:
                temporary.replace(path)
                break
            except PermissionError:
                if attempt == 6:
                    raise
                time.sleep(0.05 * 2 ** attempt)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except PermissionError:
            pass

def status(session, **details):
    value = {"session": session, "status": "PASS", "verified_at_utc": now(), **details}
    save_json(STATE / f"{session.lower()}.json", value)
    return value

def get_spark(name, memory="4g", cores=4):
    os.environ.setdefault("PYSPARK_SUBMIT_ARGS", f"--driver-memory {memory} pyspark-shell")
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("cryptopulse-" + name)
        .master(f"local[{cores}]").config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.debug.maxToStringFields", "100")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
