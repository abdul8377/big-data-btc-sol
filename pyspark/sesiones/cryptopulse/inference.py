"""S10: inferencia reutilizando las variables y los modelos validados en batch."""
import json
import math
import time
from datetime import datetime, timezone
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from .core import ML, FEATURES, STATE, save_json
from .features import past_features, pair_features, PAIR_FEATURES
from .training import calibrate
from . import db
from .signals import build_signals, context_diagnostics, current_pair_anchor

CANDLE_SCHEMA = StructType([
    StructField("symbol",StringType()),StructField("open_ms",LongType()),StructField("close_ms",LongType()),
    *[StructField(k,DoubleType()) for k in ["open","high","low","close","volume","quote_asset_volume"]],
    StructField("number_of_trades",LongType()),StructField("source_mode",StringType())
])
class ModelBundle:
    def __init__(self):
        self.registry = json.loads((ML / "model_registry.json").read_text(encoding="utf-8"))
        db.ensure_product_schema(self.registry)
        self.models = {name: PipelineModel.load(str(ML / item["model_path"]))
                       for name,item in self.registry["models"].items() if item["selected"] != "baseline"}
        self.thresholds = self.registry["thresholds"]
        self.activity = {(r["symbol"],r["hour_utc"]):r for r in self.thresholds["by_symbol_hour"]}

    def score(self, frame, name):
        item = self.registry["models"][name]
        if name == "volatility":
            baseline = F.col("rv15")
        elif name == "divergence":
            baseline = F.lit(0.0)
        else:
            rate_map = F.create_map(*[x for symbol,value in item["training_prevalence"].items()
                                     for x in (F.lit(symbol),F.lit(value))])
            baseline = rate_map[F.col("symbol")]
        result = frame.withColumn("_baseline", baseline)
        if item["selected"] == "baseline":
            return result.withColumn("_forecast", F.col("_baseline"))
        result = self.models[name].transform(result)
        if item["kind"] == "classification":
            return result.withColumn("_forecast", calibrate(item["calibration_platt"], vector_to_array("probability")[1]))
        if name == "volatility":
            return result.withColumn("_forecast", F.greatest(F.lit(0.0),F.col("prediction")))
        return result.withColumn("_forecast",F.col("prediction"))

    def predict(self, spark, candles):
        if not candles:
            return []
        values = [(r["symbol"],r["open_ms"],r["close_ms"],*[float(r[k]) for k in
            ["open","high","low","close","volume","quote_asset_volume"]],r["number_of_trades"],r["source_mode"]) for r in candles]
        raw = spark.createDataFrame(values,CANDLE_SCHEMA)
        features = past_features(raw).cache()
        latest = {}
        for r in candles:
            if r["symbol"] not in latest or r["open_ms"]>latest[r["symbol"]]["open_ms"]:
                latest[r["symbol"]]=r
        condition = F.lit(False)
        for symbol,r in latest.items():
            condition = condition | ((F.col("symbol")==symbol)&(F.col("open_ms")==r["open_ms"]))
        current = features.filter(condition).na.drop(subset=FEATURES).cache()
        kpis=[]
        for row in features.filter(condition).select("symbol","close_ms","source_mode","rv15","rv60","r1","r15","r30","quote5","trades5","hour_utc").collect():
            reference=self.activity[(row.symbol,row.hour_utc)]
            payload={k:row[k] for k in ("rv15","rv60","r1","r15","r30","quote5","trades5")}
            payload["quote5_p95"]=reference["quote5_p95"]
            payload["trades5_p95"]=reference["trades5_p95"]
            payload["source_mode"]=row.source_mode
            payload["volume_zscore"]=(row.quote5-reference["quote5_mean"])/reference["quote5_std"] if reference["quote5_std"] and row.quote5 is not None else None
            kpis.append((row.symbol,row.close_ms,payload))
        db.insert_kpis(kpis)
        outputs = []
        version = self.registry["model_version"]
        for name,horizon in [("volatility",15),("crash",30),("anomaly",5)]:
            item=self.registry["models"][name]
            scored=self.score(current,name)
            for r in scored.select("symbol","close_ms","source_mode","hour_utc","rv15","r30","quote5","trades5","_forecast","_baseline").collect():
                if name=="volatility":
                    observed=r.rv15
                    alert=r._forecast>=self.thresholds["by_symbol"][r.symbol]["volatility_alert_pct"]
                elif name=="crash":
                    observed=r.r30
                    alert=r._forecast>=item["alert_threshold"]
                else:
                    reference=self.activity[(r.symbol,r.hour_utc)]
                    observed=float(r.quote5>=reference["quote5_p95"] or r.trades5>=reference["trades5_p95"])
                    alert=r._forecast>=item["alert_threshold"]
                outputs.append((name,r.symbol,r.close_ms,horizon,float(r._forecast),float(r._baseline),
                    float(observed),version,item["selected"],r.source_mode,bool(alert)))
        pair = pair_features(features).na.drop(subset=PAIR_FEATURES)
        latest_pair=current_pair_anchor(latest)
        if latest_pair is not None:
            row=self.score(pair.filter(F.col("open_ms")==latest_pair),"divergence").select(
                "btc_close_ms","current_spread15","correlation_60m","_forecast","_baseline").first()
            if row:
                mode="live" if all(latest[s]["source_mode"]=="live" for s in ("BTCUSDT","SOLUSDT")) else "replay" if all(latest[s]["source_mode"]=="replay" for s in ("BTCUSDT","SOLUSDT")) else "backfill"
                outputs.append(("divergence","BTC-SOL",row.btc_close_ms,15,float(row._forecast),0.0,
                    float(row.current_spread15),version,self.registry["models"]["divergence"]["selected"],mode,
                    bool(abs(row._forecast)>=self.thresholds["divergence_alert_pp"])))
                pair_kpi=("BTC-SOL",row.btc_close_ms,{"correlation_60m":row.correlation_60m,"current_spread15":row.current_spread15,"source_mode":mode})
                db.insert_kpis([pair_kpi])
                kpis.append(pair_kpi)
                # Correlacion es un KPI observado, no una prediccion causal.
                save_json(STATE / "market_kpis.json",{"correlation_60m":row.correlation_60m,
                    "as_of_ms":row.btc_close_ms,"current_spread15":row.current_spread15})
        db.insert_signals(build_signals(self.registry,kpis,outputs))
        diagnostics=context_diagnostics(candles)
        self.last_status={"heartbeat_ms":int(time.time()*1000),"model_version":version,
            "status":"ready" if len(outputs)==7 else "degraded", "per_asset":diagnostics,
            "produced_series":len(outputs),"pair_synchronized":current_pair_anchor(latest) is not None,
            "reason":"Siete series calculadas" if len(outputs)==7 else
                "Datos incompletos, activos desalineados o correlación indefinida: revisar continuidad y frescura"}
        if any(r["source_mode"]!="replay" for r in candles):
            save_json(STATE/"inference_status.json",self.last_status)
        current.unpersist()
        features.unpersist()
        return outputs

def actual_for(prediction, registry, candles):
    dimension,symbol,asof,horizon = [prediction[k] for k in ("dimension","symbol","as_of_ms","horizon_minutes")]
    start=asof-59999
    mapping={(r["symbol"],r["open_ms"]):r for r in candles}
    def series(asset):
        points=[mapping.get((asset,start+i*60000)) for i in range(horizon+1)]
        return points if all(p is not None for p in points) else None
    if dimension=="divergence":
        btc,sol=series("BTCUSDT"),series("SOLUSDT")
        if not btc or not sol:
            return None
        return 100*(float(sol[-1]["close"])/float(sol[0]["close"])-float(btc[-1]["close"])/float(btc[0]["close"]))
    points=series(symbol)
    if not points:
        return None
    if dimension=="volatility":
        returns=[math.log(float(points[i]["close"])/float(points[i-1]["close"])) for i in range(1,len(points))]
        return 100*math.sqrt(sum(r*r for r in returns))
    if dimension=="crash":
        ret=100*(float(points[-1]["close"])/float(points[0]["close"])-1)
        return float(ret<=registry["thresholds"]["by_symbol"][symbol]["crash_threshold_pct"])
    hour=datetime.fromtimestamp((asof+horizon*60000)/1000,timezone.utc).hour
    reference=next(r for r in registry["thresholds"]["by_symbol_hour"] if r["symbol"]==symbol and r["hour_utc"]==hour)
    return float(sum(float(r["quote_asset_volume"]) for r in points[1:])>=reference["quote5_p95"]
                 or sum(r["number_of_trades"] for r in points[1:])>=reference["trades5_p95"])

def evaluate_matured(bundle):
    pending=db.query("""SELECT p.* FROM predictions p
        WHERE actual IS NULL AND as_of_ms+horizon_minutes*60000 <=
        (SELECT COALESCE(max(close_ms),0) FROM market_candles)
        AND (evaluation_checked_at IS NULL OR evaluation_checked_at<now()-interval '30 seconds')
        ORDER BY evaluation_checked_at NULLS FIRST,as_of_ms LIMIT 500""")
    updated=0
    registries={bundle.registry["model_version"]:bundle.registry}
    for p in pending:
        version=p["model_version"]
        if version not in registries:
            stored=db.query("SELECT registry FROM model_versions WHERE model_version=%s",(version,))
            registries[version]=stored[0]["registry"] if stored else None
        registry=registries[version]
        if registry is None:
            with db.connect() as connection,connection.cursor() as cursor:
                cursor.execute("""UPDATE predictions SET evaluation_status='unknown_model',
                    evaluation_reason='No se conserva el registro de esta versión; no se aplican umbrales de otra versión',
                    evaluation_checked_at=now() WHERE dimension=%s AND symbol=%s AND as_of_ms=%s AND model_version=%s AND actual IS NULL""",
                    (p["dimension"],p["symbol"],p["as_of_ms"],version))
            continue
        rows=db.query("SELECT * FROM market_candles WHERE open_ms BETWEEN %s AND %s",
                      (p["as_of_ms"]-59999,p["as_of_ms"]-59999+p["horizon_minutes"]*60000))
        actual=actual_for(p,registry,rows)
        if actual is None:
            with db.connect() as connection,connection.cursor() as cursor:
                cursor.execute("""UPDATE predictions SET evaluation_status='missing_data',
                    evaluation_reason='Faltan velas consecutivas dentro del horizonte; se reintentará sin imputar precios',
                    evaluation_checked_at=now() WHERE dimension=%s AND symbol=%s AND as_of_ms=%s AND model_version=%s AND actual IS NULL""",
                    (p["dimension"],p["symbol"],p["as_of_ms"],version))
            continue
        with db.connect() as connection,connection.cursor() as cursor:
            cursor.execute("""UPDATE predictions SET actual=%s,evaluated_at=now(),evaluation_status='evaluated',
                evaluation_reason=NULL,evaluation_checked_at=now()
                WHERE dimension=%s AND symbol=%s AND as_of_ms=%s AND model_version=%s AND actual IS NULL""",
                (actual,p["dimension"],p["symbol"],p["as_of_ms"],p["model_version"]))
        updated+=1
    return updated
