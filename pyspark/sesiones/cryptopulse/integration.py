"""Pruebas de integracion aisladas, con replay de velas reales y sin contaminar el vivo."""
import csv
import json
import math
import time
import uuid
from pathlib import Path
from datetime import datetime,timezone
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient,NewTopic
from pyspark.sql import SparkSession,functions as F
from pyspark.sql.types import StructType,StructField,StringType,LongType
from .core import ROOT,STATE,ML,status,save_json
from .contract import make_event,validate
from .features import past_features
from .inference import ModelBundle,actual_for,evaluate_matured,CANDLE_SCHEMA
from . import db

def real_fixture():
    path=ROOT/"s02-fundamentos/data/raw/BTCUSDT-1m-2026-01.csv"
    with path.open() as source:
        rows=[next(csv.reader(source)) for _ in range(240)]
    normalized=[]
    for row in rows:
        row[0]=int(row[0])//1000
        row[6]=int(row[6])//1000
        normalized.append(row)
    return normalized

def contract_checks(candle):
    event=make_event("BTCUSDT",candle,"replay","replay")
    validate(event)
    failures=[]
    for key,value in [("close","NaN"),("volume","-1"),("is_closed",False),
                       ("event_id","wrong"),("schema_version",99),("synthetic",True)]:
        changed=dict(event);changed[key]=value
        try:
            validate(changed)
            failures.append(key)
        except (ValueError,ArithmeticError):
            pass
    assert not failures,failures
    return {"valid_real_event":True,"invalid_variants_rejected":6}

def watermark_and_recovery(spark,candles):
    from .producer import BOOTSTRAP
    topic="cryptopulse.integration."+uuid.uuid4().hex[:12]
    admin=KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    admin.create_topics([NewTopic(topic,num_partitions=1,replication_factor=1)])
    producer=KafkaProducer(bootstrap_servers=BOOTSTRAP,acks="all",value_serializer=lambda value:json.dumps(value).encode())
    base=STATE/"integration"/topic
    checkpoint=base/"checkpoint"
    output=base/"output"
    schema=StructType([StructField("event_id",StringType()),StructField("close_ms",LongType())])
    def start():
        source=(spark.readStream.format("kafka").option("kafka.bootstrap.servers",BOOTSTRAP)
                .option("subscribe",topic).option("startingOffsets","earliest").load())
        parsed=source.select(F.from_json(F.col("value").cast("string"),schema).alias("e")).select("e.*")
        dedup=(parsed.withColumn("event_time",F.timestamp_millis("close_ms"))
            .withWatermark("event_time","2 minutes").dropDuplicatesWithinWatermark(["event_id"]))
        return (dedup.writeStream.format("parquet").outputMode("append")
            .option("checkpointLocation",str(checkpoint)).option("path",str(output))
            .trigger(processingTime="2 seconds").start())
    def send(index,repeat=1):
        event=make_event("BTCUSDT",candles[index],"replay","replay")
        for _ in range(repeat):
            producer.send(topic,key=b"BTCUSDT",value=event).get(timeout=20)
        producer.flush()
    query=start()
    try:
        send(0,2)
        query.processAllAvailable()
        assert spark.read.parquet(str(output)).count()==1
        send(10)
        query.processAllAvailable()
        assert spark.read.parquet(str(output)).count()==2
        send(1)  # Nueva clave pero tiempo anterior al watermark ya avanzado.
        query.processAllAvailable()
        assert spark.read.parquet(str(output)).count()==2
        dropped=max([sum(op.get("numRowsDroppedByWatermark",0) for op in p.get("stateOperators",[]))
                     for p in query.recentProgress] or [0])
        assert dropped>=1
    finally:
        query.stop()
    # Mismo checkpoint y mismo sink; no se reprocesan los offsets confirmados.
    send(11)
    recovered=start()
    try:
        recovered.processAllAvailable()
        assert spark.read.parquet(str(output)).count()==3
    finally:
        recovered.stop()
        producer.close()
        admin.close()
    return {"duplicate_event_counted_once":True,"late_event_rejected":True,
            "watermark_rows_dropped":dropped,"checkpoint_restart_recovered":True,"isolated_topic":topic}

def replay_model_checks(spark):
    # Solo se usan velas reales de enero 2026. source_mode=replay queda excluido en Grafana.
    all_rows=[]
    for symbol in ("BTCUSDT","SOLUSDT"):
        path=ROOT/f"s02-fundamentos/data/raw/{symbol}-1m-2026-01.csv"
        with path.open() as source:
            candles=[next(csv.reader(source)) for _ in range(190)]
        for row in candles:
            row[0]=int(row[0])//1000;row[6]=int(row[6])//1000
            event=make_event(symbol,row,"replay","replay")
            all_rows.append({**event,"number_of_trades":int(row[8])})
    anchor=min(row["open_ms"] for row in all_rows)+119*60000
    available=[row for row in all_rows if row["open_ms"]<=anchor]
    bundle=ModelBundle()
    predictions=bundle.predict(spark,available)
    assert len(predictions)==7
    # La etiqueta usa futuro, pero la prediccion anterior solo recibio filas hasta anchor.
    db.insert_candles(all_rows)
    db.insert_predictions(predictions)
    evaluated=evaluate_matured(bundle)
    results=db.query("SELECT * FROM predictions WHERE source_mode='replay' AND as_of_ms=%s AND model_version=%s",
                     (anchor+59999,bundle.registry['model_version']))
    assert len(results)==7 and all(row["actual"] is not None for row in results)
    for row in results:
        independent=actual_for(row,bundle.registry,all_rows)
        assert math.isclose(independent,row["actual"],rel_tol=1e-10,abs_tol=1e-10)
    # Prueba de no fuga: cambiar cierres posteriores no modifica variables previas.
    def to_df(values):
        payload=[(r["symbol"],r["open_ms"],r["close_ms"],*[float(r[k]) for k in
            ["open","high","low","close","volume","quote_asset_volume"]],r["number_of_trades"],r["source_mode"]) for r in values]
        return spark.createDataFrame(payload,CANDLE_SCHEMA)
    original=past_features(to_df(all_rows)).filter(F.col("open_ms")==anchor).orderBy("symbol").select("symbol","r1","rv15","rv60","quote5").collect()
    changed=[dict(r) for r in all_rows]
    for row in changed:
        if row["open_ms"]>anchor:
            row["close"]=str(float(row["close"])*1.5)
    altered=past_features(to_df(changed)).filter(F.col("open_ms")==anchor).orderBy("symbol").select("symbol","r1","rv15","rv60","quote5").collect()
    assert original==altered
    before=db.query("SELECT count(*) n FROM predictions WHERE source_mode='replay'")[0]["n"]
    db.insert_predictions(predictions)
    after=db.query("SELECT count(*) n FROM predictions WHERE source_mode='replay'")[0]["n"]
    assert before==after
    return {"replay_predictions":7,"future_labels_evaluated":7,"all_four_dimensions":True,
        "future_mutation_does_not_change_past_features":True,"prediction_upsert_idempotent":True}

def run():
    (STATE/"integration_tests.json").unlink(missing_ok=True)
    spark=(SparkSession.builder.appName("cryptopulse-integration-tests")
        .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    fixture=real_fixture()
    results={"status":"PASS","model_version":json.loads((ML/"model_registry.json").read_text())["model_version"],"verified_at_utc":datetime.now(timezone.utc).isoformat(),
        "contract":contract_checks(fixture[0]),
        "streaming":watermark_and_recovery(spark,fixture),
        "models":replay_model_checks(spark)}
    spark.stop()
    save_json(STATE/"integration_tests.json",results)
    print(json.dumps(results,indent=2))
    return results

if __name__=="__main__":
    run()
