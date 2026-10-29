"""S04/S05: cuatro dimensiones, particiones temporales y seleccion sin consultar test."""
import json
import hashlib
import math
from copy import deepcopy
from datetime import datetime, timezone
import numpy as np
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.functions import vector_to_array
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from .core import ROOT, ML, CLEAN, FEATURES, TRAIN_END, CALIBRATION_END, VALIDATION_END, get_spark, status, save_json
from .features import supervised, pair_features, PAIR_FEATURES

CALIBRATION_EMBARGO_MINUTES=30
CALIBRATION_EMBARGO_MS=CALIBRATION_EMBARGO_MINUTES*60000

def regression_metrics(df, score):
    result = df.agg(F.count("*").alias("n"),
        F.avg(F.abs(F.col(score) - F.col("label"))).alias("mae"),
        F.sqrt(F.avg(F.pow(F.col(score) - F.col("label"), 2))).alias("rmse")).first().asDict()
    return {k: int(v) if k == "n" else float(v) for k, v in result.items()}

def calibrate(parameters, probability):
    a, b = parameters
    p = F.least(F.lit(1 - 1e-6), F.greatest(F.lit(1e-6), probability))
    return 1 / (1 + F.exp(-(a * F.log(p / (1 - p)) + b)))

def fit_calibration(df):
    rows = df.select("p", "label").collect()
    y = np.asarray([r.label for r in rows], dtype=float)
    p = np.clip(np.asarray([r.p for r in rows], dtype=float), 1e-6, 1 - 1e-6)
    assert len(np.unique(y)) == 2
    x = np.column_stack([np.log(p / (1 - p)), np.ones(len(p))])
    theta = np.array([1.0, 0.0])
    for _ in range(60):
        q = 1 / (1 + np.exp(-np.clip(x @ theta, -35, 35)))
        weight = np.maximum(q * (1 - q), 1e-6)
        gradient = x.T @ (q - y) / len(y) + 1e-4 * (theta - [1, 0])
        hessian = (x.T * weight) @ x / len(y) + 1e-4 * np.eye(2)
        step = np.linalg.solve(hessian, gradient)
        theta -= 0.5 * step
        theta[0] = np.clip(theta[0], 0.05, 4)
        theta[1] = np.clip(theta[1], -10, 10)
        if np.linalg.norm(step) < 1e-7:
            break
    return theta.tolist()

def classifier_metrics(df, score, threshold):
    classified = df.withColumn("_positive", (F.col(score) >= threshold).cast("int"))
    row = classified.agg(F.count("*").alias("n"),
        F.avg(F.pow(F.col(score) - F.col("label"), 2)).alias("brier"),
        F.sum(((F.col("_positive") == 1) & (F.col("label") == 1)).cast("long")).alias("tp"),
        F.sum(((F.col("_positive") == 1) & (F.col("label") == 0)).cast("long")).alias("fp"),
        F.sum(((F.col("_positive") == 0) & (F.col("label") == 1)).cast("long")).alias("fn"),
        F.sum(((F.col("_positive") == 0) & (F.col("label") == 0)).cast("long")).alias("tn")).first()
    pr = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol=score, metricName="areaUnderPR").evaluate(df)
    bins = (df.withColumn("_bin", F.least(F.lit(9), (F.col(score) * 10).cast("int")))
        .groupBy("_bin").agg(F.count("*").alias("n"), F.avg(score).alias("mean_probability"),
             F.avg("label").alias("observed_rate")).orderBy("_bin").collect())
    return {"n": row.n, "brier": float(row.brier), "pr_auc": float(pr),
        "precision": row.tp / max(1, row.tp + row.fp), "recall": row.tp / max(1, row.tp + row.fn),
        "tp": row.tp, "fp": row.fp, "fn": row.fn, "tn": row.tn, "threshold": threshold,
        "ece": sum(r.n * abs(r.mean_probability - r.observed_rate) for r in bins) / row.n,
        "reliability_bins": [r.asDict() for r in bins]}

def select_threshold(df):
    rows = df.select("score", "label").collect()
    score = np.asarray([r.score for r in rows])
    label = np.asarray([r.label for r in rows])
    best = (0, 0.5)
    for threshold in np.unique(np.quantile(score, np.linspace(0.05, 0.995, 80))):
        predicted = score >= threshold
        tp = np.sum(predicted & (label == 1))
        fp = np.sum(predicted & (label == 0))
        fn = np.sum(~predicted & (label == 1))
        f1 = 2 * tp / max(1, 2 * tp + fp + fn)
        if f1 > best[0]:
            best = (float(f1), float(threshold))
    return best[1]

def splits(df):
    # Embargo conservador de 30 minutos, incluso para objetivos de horizonte menor.
    train = df.filter(F.col("open_ms") + 1800000 < TRAIN_END)
    validation = df.filter((F.col("open_ms") >= TRAIN_END) & (F.col("open_ms") + 1800000 < VALIDATION_END))
    test = df.filter(F.col("open_ms") >= VALIDATION_END)
    assert train.count() and validation.count() and test.count()
    return train, validation, test

def calibration_splits(validation):
    """Purge future labels before using the next interval to select thresholds.

    Calibration can observe at most 30 future minutes. Its last label must end
    strictly before selection starts; selection and test are never used to fit
    the calibrator.
    """
    calibration=validation.filter(F.col("open_ms")+CALIBRATION_EMBARGO_MS<CALIBRATION_END)
    selection=validation.filter(F.col("open_ms")>=CALIBRATION_END)
    return calibration,selection

def prepare(spark):
    signature = hashlib.sha256((ROOT / "cryptopulse/features.py").read_bytes() + (ROOT / "_runtime/s03.json").read_bytes()).hexdigest()
    prepared_path = ML / "preparation.json"
    if prepared_path.exists() and all((ML / name / "_SUCCESS").exists() for name in ("model_dataset", "pair_dataset")):
        if json.loads(prepared_path.read_text())["signature"] == signature:
            print("Reutilizando variables supervisadas ya verificadas", flush=True)
            return json.loads((ML / "thresholds.json").read_text())
    base = spark.read.parquet(str(CLEAN))
    full = supervised(base).persist(StorageLevel.DISK_ONLY)
    hist = full.filter(F.col("open_ms") + 1800000 < TRAIN_END)
    crash = hist.groupBy("symbol").agg(F.percentile_approx("future_r30", 0.05, 10000).alias("crash_threshold_pct"),
        F.percentile_approx("rv15", 0.95, 10000).alias("volatility_alert_pct"))
    activity = hist.groupBy("symbol", "hour_utc").agg(
        F.percentile_approx("quote5", 0.95, 10000).alias("quote5_p95"),
        F.percentile_approx("trades5", 0.95, 10000).alias("trades5_p95"),
        F.avg("quote5").alias("quote5_mean"), F.stddev("quote5").alias("quote5_std"))
    thresholds = {"by_symbol": {r.symbol: r.asDict() for r in crash.collect()},
                  "by_symbol_hour": [r.asDict() for r in activity.collect()]}
    save_json(ML / "thresholds.json", thresholds)
    frames = full.join(F.broadcast(crash), "symbol").join(
        F.broadcast(activity.select("symbol", F.col("hour_utc").alias("future_hour5"), "quote5_p95", "trades5_p95")),
        ["symbol", "future_hour5"])
    frames = (frames.na.drop(subset=[*FEATURES, "target_volatility", "future_r30", "future_quote5", "future_trades5"])
        .withColumn("target_crash", (F.col("future_r30") <= F.col("crash_threshold_pct")).cast("double"))
        .withColumn("target_anomaly", ((F.col("future_quote5") >= F.col("quote5_p95"))
            | (F.col("future_trades5") >= F.col("trades5_p95"))).cast("double")))
    sampled = frames.filter(F.col("open_ms") % 900000 == 0)
    sampled.select("symbol", "open_ms", "close_ms", *FEATURES, "target_volatility", "target_crash",
        "target_anomaly", "future_r15").write.mode("overwrite").parquet(str(ML / "model_dataset"))
    paired = pair_features(full, with_target=True)
    paired.filter(F.col("open_ms") % 900000 == 0).na.drop(subset=[*PAIR_FEATURES, "target_divergence"]).select(
        "open_ms", *PAIR_FEATURES, "target_divergence").write.mode("overwrite").parquet(str(ML / "pair_dataset"))
    save_json(prepared_path, {"signature": signature})
    full.unpersist()
    return thresholds

def train_regression(df, columns, target, baseline_col, name, version, compare_rf=False):
    dataset = df.withColumnRenamed(target, "label").persist(StorageLevel.MEMORY_AND_DISK)
    train, val, test = splits(dataset)
    assembler = VectorAssembler(inputCols=columns, outputCol="features", handleInvalid="error")
    candidates = {"ridge": Pipeline(stages=[assembler, LinearRegression(
        featuresCol="features", labelCol="label", regParam=0.1, elasticNetParam=0, maxIter=40)])}
    if compare_rf:
        candidates["random_forest"] = Pipeline(stages=[assembler, RandomForestRegressor(
            featuresCol="features", labelCol="label", numTrees=20, maxDepth=5, minInstancesPerNode=30, seed=26)])
    baseline_val = regression_metrics(val.withColumn("baseline", baseline_col), "baseline")
    best_name, best_rmse, best_model = "baseline", baseline_val["rmse"], None
    candidate_results = {}
    for candidate_name, pipeline in candidates.items():
        print(f"Entrenando {name}/{candidate_name}...", flush=True)
        model = pipeline.fit(train)
        scored = model.transform(val)
        if name == "volatility":
            scored = scored.withColumn("prediction", F.greatest(F.lit(0.0), F.col("prediction")))
        metrics = regression_metrics(scored, "prediction")
        candidate_results[candidate_name] = metrics
        # Se guarda tambien el candidato aunque gane el baseline.
        model_path = ML / "models" / version / (name + "_" + candidate_name)
        model.write().overwrite().save(str(model_path))
        if metrics["rmse"] < best_rmse:
            best_name, best_rmse, best_model = candidate_name, metrics["rmse"], model
    # Candidato auditado: mejor modelo ML aunque la politica despliegue baseline.
    candidate_name = min(candidate_results, key=lambda k: candidate_results[k]["rmse"])
    candidate_path = ML / "models" / version / (name + "_" + candidate_name)
    loaded = PipelineModel.load(str(candidate_path))
    test_scored = loaded.transform(test).withColumn("baseline", baseline_col)
    if name == "volatility":
        test_scored = test_scored.withColumn("prediction", F.greatest(F.lit(0.0), F.col("prediction")))
    test_scored = test_scored.withColumn("deployed", F.col("baseline") if best_name == "baseline" else F.col("prediction"))
    report = {"kind": "regression", "feature_columns": columns, "target": target,
        "selected": best_name, "candidate_name": candidate_name, "model_path": str(candidate_path.relative_to(ML)),
        "validation_baseline": baseline_val, "validation_candidates": candidate_results,
        "test_candidate": regression_metrics(test_scored, "prediction"),
        "test_baseline": regression_metrics(test_scored, "baseline"),
        "test_deployed": regression_metrics(test_scored, "deployed")}
    test_scored.select(*(["symbol"] if "symbol" in test_scored.columns else []),
        "open_ms", "label", "prediction", "baseline", "deployed").write.mode("overwrite").parquet(str(ML / "test_predictions" / name))
    report["split_rows"] = {"train": train.count(), "validation": val.count(), "test": test.count()}
    dataset.unpersist()
    return report

def train_classifier(df, target, name, version, prefit=None):
    dataset = df.withColumnRenamed(target, "label").persist(StorageLevel.MEMORY_AND_DISK)
    train, validation, test = splits(dataset)
    rates = {row.symbol: float(row.rate) for row in train.groupBy("symbol").agg(F.avg("label").alias("rate")).collect()}
    rate_map = F.create_map(*[item for symbol, value in rates.items() for item in (F.lit(symbol), F.lit(value))])
    if prefit is None:
        pipeline = Pipeline(stages=[
            VectorAssembler(inputCols=FEATURES, outputCol="features"),
            StandardScaler(inputCol="features", outputCol="scaled", withMean=True, withStd=True),
            LogisticRegression(featuresCol="scaled", labelCol="label", maxIter=60, regParam=0.01, standardization=False)])
        print(f"Entrenando {name}/logistic_regression...", flush=True)
        model = pipeline.fit(train)
        path = ML / "models" / version / name
        model.write().overwrite().save(str(path))
    else:
        path=ML/prefit["model_path"]
        print(f"Recalibrando {name}; se conserva el clasificador base {prefit['model_path']}...",flush=True)
    loaded = PipelineModel.load(str(path))
    predicted_val = loaded.transform(validation).withColumn("p", vector_to_array("probability")[1]).persist(StorageLevel.MEMORY_AND_DISK)
    calibration_frame,selection_frame=calibration_splits(predicted_val)
    calibration_range=calibration_frame.agg(F.count("*").alias("n"),
        F.min("open_ms").alias("first_open_ms"),F.max("open_ms").alias("last_open_ms")).first().asDict()
    selection_first=selection_frame.agg(F.min("open_ms").alias("first")).first().first
    assert calibration_range["n"]>0 and selection_first is not None,"Faltan datos de calibracion/seleccion"
    assert calibration_range["last_open_ms"]+CALIBRATION_EMBARGO_MS<CALIBRATION_END<=selection_first
    calibration = fit_calibration(calibration_frame)
    selection = (selection_frame
        .withColumn("score", calibrate(calibration, F.col("p"))).withColumn("baseline", rate_map[F.col("symbol")]))
    threshold = select_threshold(selection)
    val_model = classifier_metrics(selection, "score", threshold)
    val_base = classifier_metrics(selection, "baseline", 0.5)
    selected = "logistic_regression" if val_model["brier"] < val_base["brier"] else "baseline"
    scored = (loaded.transform(test).withColumn("p", vector_to_array("probability")[1])
        .withColumn("score", calibrate(calibration, F.col("p"))).withColumn("baseline", rate_map[F.col("symbol")]))
    scored = scored.withColumn("deployed", F.col("baseline") if selected == "baseline" else F.col("score"))
    deployed_threshold = 0.5 if selected == "baseline" else threshold
    report = {"kind": "classification", "feature_columns": FEATURES, "target": target, "selected": selected,
        "model_path": str(path.relative_to(ML)), "calibration_platt": calibration, "alert_threshold": deployed_threshold,
        "calibration_embargo_minutes":CALIBRATION_EMBARGO_MINUTES,"calibration_rows":calibration_range["n"],
        "calibration_range":calibration_range,"selection_first_open_ms":selection_first,
        "calibration_boundary_ms":CALIBRATION_END,"base_model_reused":prefit is not None,
        "training_prevalence": rates, "validation_candidate": val_model, "validation_baseline": val_base,
        "test_candidate": classifier_metrics(scored, "score", threshold),
        "test_baseline": classifier_metrics(scored, "baseline", 0.5),
        "test_deployed": classifier_metrics(scored, "deployed", deployed_threshold),
        "split_rows": {"train": train.count(), "validation": validation.count(), "test": test.count()}}
    output_path=ML/"test_predictions"/version/name if prefit is not None else ML/"test_predictions"/name
    scored.select("symbol", "open_ms", "label", "score", "baseline", "deployed").write.mode("overwrite").parquet(str(output_path))
    report["test_predictions_path"]=str(output_path.relative_to(ML))
    predicted_val.unpersist()
    dataset.unpersist()
    return report

def run():
    spark = get_spark("s04-modelos")
    ML.mkdir(parents=True, exist_ok=True)
    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    thresholds = prepare(spark)
    common = spark.read.parquet(str(ML / "model_dataset"))
    pairs = spark.read.parquet(str(ML / "pair_dataset"))
    reports = {}
    reports["volatility"] = train_regression(common, FEATURES, "target_volatility", F.col("rv15"), "volatility", version, True)
    reports["crash"] = train_classifier(common, "target_crash", "crash", version)
    reports["anomaly"] = train_classifier(common, "target_anomaly", "anomaly", version)
    reports["divergence"] = train_regression(pairs, PAIR_FEATURES, "target_divergence", F.lit(0.0), "divergence", version)
    hist_pair = pairs.filter(F.col("open_ms") + 1800000 < TRAIN_END)
    thresholds["divergence_alert_pp"] = float(hist_pair.select(F.abs("target_divergence").alias("value")).approxQuantile("value", [0.95], 0.001)[0])
    save_json(ML / "thresholds.json", thresholds)
    registry = {"model_version": version, "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "train_end_exclusive_ms": TRAIN_END, "calibration_end_ms": CALIBRATION_END,
        "validation_end_exclusive_ms": VALIDATION_END, "embargo_minutes": 30,
        "calibration_embargo_minutes":CALIBRATION_EMBARGO_MINUTES,
        "training_stride_minutes": 15, "prediction_cadence_minutes": 1,
        "selection_rule": "Validacion RMSE o Brier; se conserva baseline si no se mejora. Test no interviene en seleccion.",
        "thresholds": thresholds, "models": reports}
    save_json(ML / "model_registry.json", registry)
    report = status("S04", model_version=version, dimensions=list(reports),
        test_metrics={name: r["test_deployed"] for name, r in reports.items()},
        deployed={name: r["selected"] for name, r in reports.items()},
        temporal_validation=True, models_reloaded=True)
    save_json(ML / "verificacion_s04.json", report)
    spark.stop()
    return report

def recalibrate():
    """Re-evaluate saved classifiers with purged calibration, without refitting ML.

    Regression reports and model directories remain as recorded. A new registry
    version is published only after both classifiers and all reports are ready.
    """
    current_path=ML/"model_registry.json"
    base=json.loads(current_path.read_text(encoding="utf-8"))
    data_path=ML/"model_dataset"
    if not (data_path/"_SUCCESS").exists():
        raise ValueError("Falta model_dataset; no se reconstruye durante --recalibrate")
    snapshots=ML/"registries"
    snapshots.mkdir(parents=True,exist_ok=True)
    old_snapshot=snapshots/(base["model_version"]+".json")
    if not old_snapshot.exists():
        save_json(old_snapshot,base)
    version=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")+".cal"
    registry=deepcopy(base)
    registry.update({"model_version":version,"created_at_utc":datetime.now(timezone.utc).isoformat(),
        "base_model_version":base["model_version"],"calibration_embargo_minutes":CALIBRATION_EMBARGO_MINUTES,
        "recalibration_only":True,
        "recalibration_scope":"Calibracion y seleccion de los clasificadores con embargo temporal. Regresores conservados.",
        "selection_rule":"Calibracion purgada 30 minutos antes de seleccion. Seleccion por Brier en 2025; test solo evalua y no selecciona."})
    spark=get_spark("s04-recalibracion",memory="2g",cores=2)
    try:
        dataset=spark.read.parquet(str(data_path))
        for name,target in (("crash","target_crash"),("anomaly","target_anomaly")):
            registry["models"][name]=train_classifier(dataset,target,name,version,prefit=base["models"][name])
            registry["models"][name]["base_model_version"]=base["model_version"]
    finally:
        spark.stop()
    save_json(snapshots/(version+".json"),registry)
    report=status("S04",model_version=version,base_model_version=base["model_version"],
        dimensions=list(registry["models"]),recalibration_only=True,calibration_embargo_minutes=CALIBRATION_EMBARGO_MINUTES,
        test_metrics={name:item["test_deployed"] for name,item in registry["models"].items()},
        deployed={name:item["selected"] for name,item in registry["models"].items()},
        temporal_validation=True,models_reloaded=True)
    save_json(ML/"verificacion_s04.json",report)
    save_json(ML/("recalibration_"+version+".json"),report)
    # Commit point: readers see the complete previous or complete new registry.
    save_json(current_path,registry)
    return report

def verify_u1():
    registry = json.loads((ML / "model_registry.json").read_text(encoding="utf-8"))
    quality = json.loads((ROOT / "_runtime/s03.json").read_text(encoding="utf-8"))
    assert quality["status"] == "PASS" and quality["clean_rows"] > 1_000_000
    for name in ("volatility", "crash", "anomaly", "divergence"):
        item = registry["models"][name]
        assert (ML / item["model_path"] / "metadata").exists()
        assert item["test_deployed"]["n"] > 0
        assert all(item["split_rows"][key] > 0 for key in ("train", "validation", "test"))
    return status("S05", unit=1, quality_rows=quality["clean_rows"],
        model_version=registry["model_version"], dimensions=4, persisted_and_evaluated=True)

if __name__ == "__main__":
    import sys
    result = verify_u1() if "--verify-u1" in sys.argv else recalibrate() if "--recalibrate" in sys.argv else run()
    print(json.dumps(result, indent=2, default=str))
