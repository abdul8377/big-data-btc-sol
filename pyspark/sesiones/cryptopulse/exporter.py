"""S09: metricas reales y API local de estado, sin acceso al socket Docker."""
import json
import math
import os
import time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from prometheus_client import CollectorRegistry,Gauge,generate_latest,CONTENT_TYPE_LATEST
from kafka import KafkaConsumer,TopicPartition
from . import db
from .core import ROOT,STATE,ML

SYMBOLS=("BTCUSDT","SOLUSDT")
EXPECTED_SERIES=[(dimension,symbol) for dimension in ("volatility","crash","anomaly")
                 for symbol in SYMBOLS]+[("divergence","BTC-SOL")]
MAX_DATA_AGE_SECONDS=180
MAX_PREDICTION_LAG_MINUTES=2

def build_health(candle_rows, context_rows, prediction_rows, stream, producer, model_version,
                 current_ms=None, pending_evaluations=None):
    """Pure health policy shared by API, monitoring and end-to-end verification.

    A live driver and fresh candles do not imply usable inference. Every current
    model/asset series must be fresh and have a complete one-hour context.
    """
    current_ms=int(time.time()*1000) if current_ms is None else int(current_ms)
    issues=[]
    def issue(code,detail):
        issues.append({"code":code,"detail":detail})
    def age(value):
        return round((current_ms-value)/1000,3) if value is not None else None
    stream_age=age(stream.get("heartbeat_ms"))
    if stream_age is None or stream_age>60 or stream_age < -10:
        issue("stream_stale","Spark no publica una señal de vida reciente.")
    if not stream.get("inference_enabled"):
        issue("inference_disabled","La inferencia está deshabilitada o aún no inició.")
    if not model_version or stream.get("model_version")!=model_version:
        issue("model_mismatch","El proceso y el registro no confirman la misma versión de modelo.")
    producer_age=age(producer.get("heartbeat_ms"))
    if producer_age is None or producer_age>60 or producer_age < -10:
        issue("producer_stale","El productor no actualiza su estado.")
    indexed={r["symbol"]:r for r in candle_rows}
    assets=[]
    for symbol in SYMBOLS:
        row=indexed.get(symbol,{})
        times=sorted({int(r["open_ms"]) for r in context_rows if r["symbol"]==symbol})[-61:]
        missing=sum(max(0,(later-earlier)//60000-1) for earlier,later in zip(times,times[1:]))
        latest=row.get("last_close_ms")
        asset_age=age(latest)
        reasons=[]
        if len(times)<61:
            reasons.append("warmup")
        if missing:
            reasons.append("gap")
        if latest is None or asset_age>MAX_DATA_AGE_SECONDS or asset_age < -10:
            reasons.append("stale")
        ready=not reasons
        assets.append({"symbol":symbol,"last_close_ms":latest,"age_seconds":asset_age,
                       "context_rows":len(times),"missing_minutes":missing,"context_ready":ready,
                       "status":"ready" if ready else "degraded","reasons":reasons})
        if reasons:
            issue("market_"+symbol,"%s: %s; %s minutos ausentes en el contexto reciente." %
                  (symbol,", ".join(reasons),missing))
    indexed_predictions={(r["dimension"],r["symbol"]):r for r in prediction_rows
                         if r.get("model_version")==model_version}
    series=[]
    for dimension,symbol in EXPECTED_SERIES:
        row=indexed_predictions.get((dimension,symbol),{})
        timestamp=row.get("as_of_ms")
        series_age=age(timestamp)
        relevant=SYMBOLS if symbol=="BTC-SOL" else [symbol]
        candle_times=[indexed.get(asset,{}).get("last_close_ms") for asset in relevant]
        reference=min(candle_times) if all(value is not None for value in candle_times) else None
        lag=round((reference-timestamp)/60000,3) if reference is not None and timestamp is not None else None
        reasons=[]
        if timestamp is None:
            reasons.append("missing")
        elif series_age>MAX_DATA_AGE_SECONDS or series_age < -10:
            reasons.append("stale")
        if lag is not None and lag>MAX_PREDICTION_LAG_MINUTES:
            reasons.append("behind_market")
        if any(not asset["context_ready"] for asset in assets if asset["symbol"] in relevant):
            reasons.append("incomplete_context")
        series.append({"dimension":dimension,"symbol":symbol,"as_of_ms":timestamp,
                       "age_seconds":series_age,"lag_minutes":lag,
                       "model_version":row.get("model_version"),
                       "status":"ready" if not reasons else "degraded","reasons":reasons})
        if reasons:
            issue("prediction_"+dimension+"_"+symbol,"%s/%s: %s" % (dimension,symbol,", ".join(reasons)))
    ready=not issues
    return {"ready":ready,"status":"ready" if ready else "degraded","checked_at_ms":current_ms,
            "active_model_version":model_version,"stream_age_seconds":stream_age,
            "producer_age_seconds":producer_age,"symbols":assets,"prediction_series":series,
            "reasons":issues,"pending_evaluations":pending_evaluations or [],
            "policy":{"context_candles":61,"max_data_age_seconds":MAX_DATA_AGE_SECONDS,
                      "max_prediction_lag_minutes":MAX_PREDICTION_LAG_MINUTES}}

def product_health(candles=None,predictions=None,stream=None,producer=None):
    version=read_json(ML/"model_registry.json").get("model_version")
    if candles is None:
        candles=db.query("SELECT symbol,count(*) candles,max(close_ms) last_close_ms FROM market_candles WHERE source_mode<>'replay' GROUP BY symbol")
    if predictions is None:
        predictions=db.query("""SELECT DISTINCT ON(dimension,symbol) dimension,symbol,as_of_ms,model_version
            FROM predictions WHERE source_mode='live' AND model_version=%s
            ORDER BY dimension,symbol,as_of_ms DESC""",(version,))
    context=db.query("""SELECT symbol,open_ms FROM (SELECT symbol,open_ms,
        row_number() OVER(PARTITION BY symbol ORDER BY open_ms DESC) rn
        FROM market_candles WHERE source_mode<>'replay') c WHERE rn<=61""")
    pending=db.query("""SELECT dimension,count(*) n,min(as_of_ms) oldest_as_of_ms FROM predictions
        WHERE actual IS NULL AND source_mode='live' AND as_of_ms+horizon_minutes*60000
        < extract(epoch FROM now())*1000 GROUP BY dimension ORDER BY dimension""")
    return build_health(candles,context,predictions,stream if stream is not None else read_json(STATE/"stream_status.json"),
                        producer if producer is not None else read_json(STATE/"producer_status.json"),version,
                        pending_evaluations=pending)

def read_json(path,default=None):
    try:
        return json.loads(path.read_text())
    except (OSError,ValueError):
        return default if default is not None else {}

def signal_snapshot():
    rows=db.query("SELECT * FROM cp_signal_board ORDER BY dimension,symbol,kind")
    alerts=[row for row in rows if row['state'] in ('ALERTA','VIGILANCIA')]
    unavailable=any(row['state'] in ('DESACTUALIZADO','SIN EVALUAR','NO DISPONIBLE') for row in rows)
    state='degraded' if unavailable or len(rows)!=14 else 'alerts' if alerts else 'no_alerts'
    messages={'degraded':'Faltan evaluaciones recientes; revisar salud antes de interpretar riesgo.',
              'alerts':'Hay cruces reales de umbral; contrastar las dimensiones y su contexto.',
              'no_alerts':'Evaluaciones recientes sin cruces de umbral. No equivale a ausencia de riesgo.'}
    return {'state':state,'message':messages[state],'current':rows,'active_alerts':alerts}

def snapshot():
    candles=db.query("SELECT symbol,count(*) candles,max(close_ms) last_close_ms FROM market_candles WHERE source_mode<>'replay' GROUP BY symbol")
    version=read_json(ML/"model_registry.json").get("model_version")
    predictions=db.query("""SELECT DISTINCT ON(dimension,symbol) dimension,symbol,as_of_ms,forecast,observed,method,alert,actual,source_mode,model_version
        FROM predictions WHERE source_mode='live' AND model_version=%s
        ORDER BY dimension,symbol,as_of_ms DESC,issued_at DESC""",(version,))
    producer,stream=read_json(STATE/"producer_status.json"),read_json(STATE/"stream_status.json")
    return {"producer":producer,"stream":stream,"candles":candles,"predictions":predictions,
        "health":product_health(candles,predictions,stream,producer),"signals":signal_snapshot(),
        "sessions":[read_json(p) for p in sorted(STATE.glob("s[0-9][0-9].json"))]}

def metrics():
    registry=CollectorRegistry()
    def gauge(name,description,value,labels=None):
        labels=labels or {}
        item=Gauge("cryptopulse_"+name,description,list(labels),registry=registry)
        return item
    # Se crean familias una vez por captura.
    def family(name,description,labels):
        return Gauge("cryptopulse_"+name,description,labels,registry=registry)
    current=time.time()*1000
    producer=read_json(STATE/"producer_status.json")
    stream=read_json(STATE/"stream_status.json")
    family("producer_connected","WebSocket conectado segun el productor",[]).set(int(producer.get("connected",False)))
    family("producer_heartbeat_age_seconds","Edad del heartbeat del productor",[]).set(max(0,(current-producer.get("heartbeat_ms",0))/1000))
    family("websocket_age_seconds","Segundos desde el ultimo mensaje WebSocket",[]).set(max(0,(current-producer.get("last_websocket_ms",0))/1000))
    family("producer_events_total","Eventos confirmados por Kafka desde el estado persistido",[]).set(producer.get("published",0))
    family("stream_heartbeat_age_seconds","Edad del heartbeat del driver Spark",[]).set(max(0,(current-stream.get("heartbeat_ms",0))/1000))
    family("inference_enabled","Inferencia habilitada en el job Spark",[]).set(int(stream.get("inference_enabled",False)))
    health=product_health(stream=stream,producer=producer)
    family("product_ready","Datos continuos e inferencia reciente de las siete series",[]).set(int(health["ready"]))
    missing=family("recent_missing_minutes","Minutos ausentes entre las ultimas 61 velas por activo",["symbol"])
    context=family("context_ready","Contexto completo y reciente para calcular las variables",["symbol"])
    prediction_age=family("prediction_age_seconds","Edad de cada pronostico de la version activa",["dimension","symbol"])
    pending=family("matured_predictions_pending","Predicciones live maduras aun sin resultado observado",["dimension"])
    for asset in health["symbols"]:
        missing.labels(asset["symbol"]).set(asset["missing_minutes"])
        context.labels(asset["symbol"]).set(int(asset["context_ready"]))
    for prediction in health["prediction_series"]:
        prediction_age.labels(prediction["dimension"],prediction["symbol"]).set(
            prediction["age_seconds"] if prediction["age_seconds"] is not None else float("inf"))
    pending_index={row["dimension"]:row["n"] for row in health["pending_evaluations"]}
    for dimension in ("volatility","crash","anomaly","divergence"):
        pending.labels(dimension).set(pending_index.get(dimension,0))
    duration=family("batch_duration_seconds","Duracion del ultimo micro-lote Spark",["query"])
    throughput=family("input_rows_per_second","Tasa de entrada informada por Spark",["query"])
    dropped=family("watermark_dropped_rows","Filas descartadas por watermark en el ultimo progreso",["query"])
    available_offsets={}
    processed={}
    for query,progress in stream.get("queries",{}).items():
        if not progress:
            continue
        duration.labels(query).set(progress.get("durationMs",{}).get("triggerExecution",0)/1000)
        throughput.labels(query).set(progress.get("inputRowsPerSecond",0))
        dropped.labels(query).set(sum(op.get("numRowsDroppedByWatermark",0) for op in progress.get("stateOperators",[])))
        if query=="cryptopulse_archive":
            for source in progress.get("sources",[]):
                offsets=source.get("endOffset",{})
                if isinstance(offsets,str):
                    offsets=json.loads(offsets)
                processed.update(offsets or {})
    lag=family("kafka_checkpoint_lag","Offsets broker menos offsets procesados por Spark",["topic"])
    probe=family("kafka_probe_up","Consulta real de offsets de Kafka",[])
    try:
        consumer=KafkaConsumer(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP","kafka:9092"),
            enable_auto_commit=False,client_id="cryptopulse-offset-probe",request_timeout_ms=10000)
        partitions=[]
        for topic in ("market.klines.btcusdt","market.klines.solusdt"):
            partitions.extend(TopicPartition(topic,p) for p in (consumer.partitions_for_topic(topic) or []))
        offsets=consumer.end_offsets(partitions)
        for topic in ("market.klines.btcusdt","market.klines.solusdt"):
            difference=sum(max(0,end-int(processed.get(topic,{}).get(str(tp.partition),0)))
                           for tp,end in offsets.items() if tp.topic==topic)
            lag.labels(topic).set(difference)
        consumer.close()
        probe.set(1)
    except Exception:
        probe.set(0)
    fresh=family("last_candle_age_seconds","Antiguedad de la ultima vela cerrada",["symbol"])
    count=family("archived_candles","Velas unicas persistidas",["symbol"])
    for row in db.query("SELECT symbol,count(*) n,max(close_ms) last_ms FROM market_candles GROUP BY symbol"):
        fresh.labels(row["symbol"]).set(max(0,(current-row["last_ms"])/1000))
        count.labels(row["symbol"]).set(row["n"])
    error=family("rejected_events","Eventos rechazados persistidos",[])
    error.set(db.query("SELECT count(*) n FROM rejected_events")[0]["n"])
    evaluated=family("evaluated_predictions","Predicciones con resultado futuro ya disponible",["dimension","source_mode"])
    for row in db.query("SELECT dimension,source_mode,count(*) n FROM predictions WHERE actual IS NOT NULL GROUP BY dimension,source_mode"):
        evaluated.labels(row["dimension"],row["source_mode"]).set(row["n"])
    cost=read_json(ROOT/"_runtime/costs.json")
    family("estimated_monthly_cost_usd","Estimacion ilustrativa configurable, no factura",[]).set(cost.get("monthly_usd",0))
    return generate_latest(registry)

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            response_code=200
            if self.path.startswith("/metrics"):
                payload=metrics(); content_type=CONTENT_TYPE_LATEST
            elif self.path.split("?",1)[0] in ("/health/live","/live"):
                payload=b'{"status":"ok"}';content_type="application/json"
            elif self.path.split("?",1)[0] in ("/health","/ready"):
                health=product_health()
                response_code=200 if health["ready"] else 503
                payload=json.dumps(health,default=str).encode();content_type="application/json"
            elif self.path.startswith("/api/signals"):
                payload=json.dumps(signal_snapshot(),default=str).encode();content_type="application/json"
            elif self.path.startswith("/api/status"):
                payload=json.dumps(snapshot(),default=str).encode();content_type="application/json"
            else:
                payload=b'{"service":"CryptoPulse","status":"/api/status","metrics":"/metrics"}';content_type="application/json"
            self.send_response(response_code);self.send_header("Content-Type",content_type);self.end_headers();self.wfile.write(payload)
        except Exception as error:
            self.send_response(503);self.end_headers();self.wfile.write(json.dumps({"error":str(error)}).encode())
    def log_message(self,*_):
        pass

if __name__=="__main__":
    ThreadingHTTPServer(("0.0.0.0",8000),Handler).serve_forever()
