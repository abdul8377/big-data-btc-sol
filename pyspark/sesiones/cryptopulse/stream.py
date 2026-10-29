"""S08/S10: Kafka -> Spark Structured Streaming -> PostgreSQL, con estado durable."""
import hashlib
import json
import os
import signal
import time
import traceback
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType,StructField,StringType,LongType,BooleanType
from psycopg2.extras import execute_values,Json
from cryptopulse.core import STATE,save_json
from cryptopulse.contract import validate
from cryptopulse import db

BOOTSTRAP=os.environ.get("KAFKA_BOOTSTRAP","kafka:9092")
TOPICS="market.klines.btcusdt,market.klines.solusdt"
INFERENCE=os.environ.get("INFERENCE_ENABLED","false").lower()=="true"
running=True

def valid_market_payload(raw):
    """Use the same contract in the archive and both window queries."""
    try:
        validate(json.loads(raw))
        return True
    except (ValueError,KeyError,TypeError,ArithmeticError):
        return False

def run():
    global running
    spark=(SparkSession.builder.appName("cryptopulse-live")
        .config("spark.sql.session.timeZone","UTC")
        .config("spark.sql.shuffle.partitions","4")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled","true")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    bundle=None
    if INFERENCE:
        from cryptopulse.inference import ModelBundle
        bundle=ModelBundle()
    source=(spark.readStream.format("kafka").option("kafka.bootstrap.servers",BOOTSTRAP)
        .option("subscribe",TOPICS).option("startingOffsets","earliest")
        .option("maxOffsetsPerTrigger",2000).option("failOnDataLoss","true").load())
    text=source.selectExpr("CAST(value AS STRING) AS raw")
    def archive_batch(batch,batch_id):
        if batch.isEmpty():
            return
        started=time.monotonic()
        events,rejected=[],[]
        for row in batch.select("raw").collect():
            try:
                event=validate(json.loads(row.raw))
                events.append(event)
            except (ValueError,KeyError,TypeError,ArithmeticError) as error:
                # A Kafka tombstone must be rejected without crashing the archive query.
                rejected.append((hashlib.sha256(str(row.raw).encode()).hexdigest(),str(error),Json({"raw":row.raw})))
        inserted=db.insert_candles(events)
        if rejected:
            with db.connect() as connection,connection.cursor() as cursor:
                execute_values(cursor,"INSERT INTO rejected_events(event_hash,reason,payload) VALUES %s ON CONFLICT DO NOTHING",rejected)
        if bundle and events:
            from cryptopulse.inference import evaluate_matured
            predictions=bundle.predict(spark,db.recent_candles())
            db.insert_predictions(predictions)
            evaluate_matured(bundle)
        duration=(time.monotonic()-started)*1000
        with db.connect() as connection,connection.cursor() as cursor:
            cursor.execute("""INSERT INTO stream_batches(query_name,batch_id,rows_count,duration_ms,payload)
                VALUES('archive',%s,%s,%s,%s) ON CONFLICT(query_name,batch_id) DO UPDATE SET
                rows_count=EXCLUDED.rows_count,duration_ms=EXCLUDED.duration_ms,payload=EXCLUDED.payload,updated_at=now()""",
                (batch_id,len(events)+len(rejected),duration,Json({"inserted_unique":inserted,"rejected":len(rejected)})))
        print(json.dumps({"batch":batch_id,"received":len(events),"inserted":inserted,"inference":INFERENCE,"duration_ms":duration}),flush=True)
    raw_query=(text.writeStream.queryName("cryptopulse_archive").foreachBatch(archive_batch)
        .outputMode("append").option("checkpointLocation",str(STATE/"checkpoints/archive"))
        .trigger(processingTime="15 seconds").start())
    fields=[StructField(k,StringType()) for k in ["event_id","event_type","symbol","interval","source_mode","source","transport",
        "open","high","low","close","volume","quote_asset_volume"]]
    fields += [StructField(k,LongType()) for k in ["open_ms","close_ms","number_of_trades","schema_version"]]
    fields += [StructField("is_closed",BooleanType()),StructField("synthetic",BooleanType())]
    accepted=text.filter(F.udf(valid_market_payload,BooleanType())(F.col("raw")))
    parsed=accepted.select(F.from_json("raw",StructType(fields)).alias("e")).select("e.*")
    for name in ["open","high","low","close","volume","quote_asset_volume"]:
        parsed=parsed.withColumn("_"+name,F.expr(f"try_cast({name} AS DOUBLE)"))
    valid=parsed.filter(
        (F.col("schema_version")==1)&(F.col("event_type")=="market.candle.closed")
        &F.col("symbol").isin("BTCUSDT","SOLUSDT")&(F.col("interval")=="1m")
        &(F.col("source")=="binance_spot")&F.col("source_mode").isin("live","backfill","replay")
        &F.col("transport").isin("websocket","rest","replay")
        &(F.col("is_closed")==True)&(F.col("synthetic")==False)
        &(F.col("open_ms")%60000==0)&(F.col("close_ms")-F.col("open_ms")==59999)
        &(F.col("event_id")==F.concat_ws(":",F.col("symbol"),F.lit("1m"),F.col("open_ms").cast("string")))
        &(F.least("_open","_high","_low","_close")>0)&(F.col("_high")>=F.greatest("_open","_close"))
        &(F.col("_low")<=F.least("_open","_close"))&(F.col("_high")>=F.col("_low"))
        &(F.col("_volume")>=0)&(F.col("_quote_asset_volume")>=0)&(F.col("number_of_trades")>=0)
        &(F.col("close_ms")<=F.unix_millis(F.current_timestamp())+10000)
        &F.col("_close").isNotNull()&(~F.isnan("_close"))
    ).withColumn("event_timestamp",F.timestamp_millis("close_ms")).withWatermark("event_timestamp","2 minutes")
    def windows_batch(kind):
        def write(batch,batch_id):
            if batch.isEmpty():
                return
            values=[(kind,r.symbol,r.window.start,r.window.end,r.candles,r.quote_volume_usdt,r.avg_close_usdt,True)
                    for r in batch.collect()]
            with db.connect() as connection,connection.cursor() as cursor:
                execute_values(cursor,"""INSERT INTO stream_windows(kind,symbol,window_start,window_end,candles,
                    quote_volume_usdt,avg_close_usdt,final) VALUES %s ON CONFLICT(kind,symbol,window_start)
                    DO UPDATE SET candles=EXCLUDED.candles,quote_volume_usdt=EXCLUDED.quote_volume_usdt,
                    avg_close_usdt=EXCLUDED.avg_close_usdt,final=EXCLUDED.final""",values)
        return write
    queries=[raw_query]
    for kind,step in [("fixed","5 minutes"),("sliding","1 minute")]:
        aggregated=(valid.dropDuplicatesWithinWatermark(["event_id"])
            .groupBy(F.window("event_timestamp","5 minutes",step),"symbol").agg(
                F.count("*").alias("candles"),F.sum("_quote_asset_volume").alias("quote_volume_usdt"),
                F.avg("_close").alias("avg_close_usdt")))
        query=(aggregated.writeStream.queryName("cryptopulse_"+kind).outputMode("append")
            .foreachBatch(windows_batch(kind)).option("checkpointLocation",str(STATE/("checkpoints/"+kind)))
            .trigger(processingTime="15 seconds").start())
        queries.append(query)
    def stop(*_):
        global running
        running=False
    signal.signal(signal.SIGTERM,stop)
    try:
        while running:
            for query in queries:
                if query.exception():
                    raise query.exception()
                if not query.isActive:
                    raise RuntimeError("Una consulta de streaming se detuvo")
            progress={q.name:q.lastProgress for q in queries}
            save_json(STATE/"stream_status.json",{"heartbeat_ms":int(time.time()*1000),"inference_enabled":INFERENCE,
                "queries":progress,"model_version":bundle.registry["model_version"] if bundle else None})
            time.sleep(5)
    finally:
        for query in queries:
            query.stop()
        spark.stop()

if __name__=="__main__":
    run()
