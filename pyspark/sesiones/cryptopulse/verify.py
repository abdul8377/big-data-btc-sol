"""Comprobaciones funcionales de ingesta, infraestructura y cierre de unidad."""
import json
import os
import time
import uuid
from pathlib import Path
import requests
from kafka import KafkaProducer,KafkaConsumer,TopicPartition
from psycopg2.extras import Json
from .core import ROOT,STATE,ML,status,save_json
from .contract import validate
from . import db

def kafka_smoke():
    from .producer import ensure_topics
    ensure_topics()
    bootstrap=os.environ.get("KAFKA_BOOTSTRAP","kafka:9092")
    producer=KafkaProducer(bootstrap_servers=bootstrap,acks="all",value_serializer=lambda x:json.dumps(x).encode())
    probe={"probe_id":uuid.uuid4().hex,"purpose":"s06_integration_test"}
    receipt=producer.send("cryptopulse.integration",key=b"contract-probe",value=probe).get(timeout=20)
    consumer=KafkaConsumer(bootstrap_servers=bootstrap,enable_auto_commit=False,request_timeout_ms=15000)
    partition=TopicPartition(receipt.topic,receipt.partition)
    consumer.assign([partition]);consumer.seek(partition,receipt.offset)
    found=False
    until=time.monotonic()+15
    while time.monotonic()<until and not found:
        for messages in consumer.poll(timeout_ms=1000).values():
            for message in messages:
                found=found or json.loads(message.value).get("probe_id")==probe["probe_id"]
    consumer.close();producer.close()
    assert found,"El mensaje de prueba no pudo leerse en Kafka"
    return status("S06",kafka_roundtrip=True,partition=receipt.partition,offset=receipt.offset,contract_version=1)

def verify_market_ingestion():
    info=json.loads((STATE/"producer_status.json").read_text())
    assert info.get("websocket_messages",0)>0,"Aun no se recibieron mensajes WebSocket"
    assert info.get("live_published",0)>0
    assert info.get("connected"),"WebSocket desconectado; revisar respaldo REST y reconexion"
    assert 0 <= time.time()*1000-info.get("heartbeat_ms",0) < 60000,"Estado del productor atrasado"
    assert 0 <= time.time()*1000-info.get("last_websocket_ms",0) < 90000,"No llegan mensajes WebSocket recientes"
    bootstrap=os.environ.get("KAFKA_BOOTSTRAP","kafka:9092")
    consumer=KafkaConsumer(bootstrap_servers=bootstrap,enable_auto_commit=False,request_timeout_ms=15000)
    checked={}
    for symbol in ("BTCUSDT","SOLUSDT"):
        topic="market.klines."+symbol.lower()
        available=consumer.partitions_for_topic(topic)
        assert available,f"No hay particiones disponibles para {topic}"
        partitions=[TopicPartition(topic,p) for p in available]
        ends=consumer.end_offsets(partitions)
        found=None
        for partition,end in ends.items():
            if end<=0:
                continue
            consumer.assign([partition]);consumer.seek(partition,end-1)
            for records in consumer.poll(timeout_ms=2000).values():
                for message in records:
                    event=validate(json.loads(message.value))
                    assert message.key.decode()==symbol
                    found={"partition":message.partition,"offset":message.offset,"event_id":event["event_id"],"transport":event["transport"]}
        assert found
        checked[symbol]=found
    consumer.close()
    return status("S07",public_websocket_verified=True,market_topics=checked,
        adaptation="Telemetria real de mercado del brief; no sensores fisicos simulados.")

def verify_streaming():
    counts=db.query("SELECT symbol,count(*) n FROM market_candles GROUP BY symbol")
    assert len(counts)==2 and all(r["n"]>=120 for r in counts)
    windows=db.query("SELECT kind,count(*) n FROM stream_windows GROUP BY kind")
    assert {r["kind"] for r in windows}=={"fixed","sliding"}
    for name in ("archive","fixed","sliding"):
        path=STATE/"checkpoints"/name
        assert (path/"commits").exists() and any((path/"commits").iterdir())
        if name!="archive":
            assert (path/"state").exists()
    result=status("S08",archived_rows=counts,window_rows=windows,watermark="2 minutes",
        sliding="5 minutes / 1 minute",fixed="5 minutes",checkpoints=True,
        delivery="At-least-once con claves idempotentes en PostgreSQL y deduplicacion acotada en ventanas")
    return result

def verify_observability():
    targets=requests.get("http://prometheus:9090/api/v1/targets",timeout=15).json()["data"]["activeTargets"]
    active={row["labels"]["job"]:row["health"] for row in targets}
    assert all(active.get(job)=="up" for job in ("cryptopulse","kafka","prometheus")),active
    rules=requests.get("http://prometheus:9090/api/v1/rules",timeout=15).json()["data"]["groups"]
    alerts=sum(len(group["rules"]) for group in rules)
    assert alerts>=5
    costs=json.loads((STATE/"costs.json").read_text())
    assert costs["kind"]=="illustrative_estimate" and costs["monthly_usd"]>=0
    return status("S09",prometheus_targets=active,alert_rules=alerts,monthly_estimate_usd=costs["monthly_usd"],
        cost_kind="Tarifas ilustrativas editables; no factura ni cotizacion.")

def verify_inference():
    from .exporter import product_health
    stream=json.loads((STATE/"stream_status.json").read_text())
    assert stream["inference_enabled"]
    registry=json.loads((ML/"model_registry.json").read_text())
    version=registry["model_version"]
    rows=db.query("""SELECT dimension,symbol,count(*) n,max(as_of_ms) latest FROM predictions
        WHERE source_mode='live' AND model_version=%s GROUP BY dimension,symbol""",(version,))
    assert {row["dimension"] for row in rows}=={"volatility","crash","anomaly","divergence"},rows
    assert len(rows)==7
    health=product_health()
    assert health["ready"],{"reason":"El producto no esta listo; no basta con que existan predicciones antiguas", "health":health}
    return status("S10",model_version=stream["model_version"],live_predictions=rows,
        horizons_minutes={"volatility":15,"crash":30,"anomaly":5,"divergence":15},health=health)

def grafana_auth():
    return ("admin",os.environ.get("GRAFANA_PASSWORD","admin2026!"))

def validate_panel_result(panel,item):
    assert "error" not in item,(panel["title"],item)
    assert item.get("status",200)<400,(panel["title"],item)
    frames=item.get("frames",[])
    for frame in frames:
        assert not frame.get("schema",{}).get("meta",{}).get("error"),(panel["title"],frame)
    row_count=sum(len(frame.get("data",{}).get("values",[[]])[0])
                  for frame in frames if frame.get("data",{}).get("values"))
    if panel.get("id")==10:
        assert row_count>0,"El panel de alertas debe explicar sin alertas/datos atrasados, no quedar en No data"
    return {"panel":panel["title"],"frames":len(frames),"rows":row_count}

def verify_bi():
    base="http://grafana:3000"
    for uid in ("cryptopulse-postgres","cryptopulse-prometheus"):
        response=requests.get(base+"/api/datasources/uid/"+uid+"/health",auth=grafana_auth(),timeout=20)
        assert response.status_code==200,(uid,response.text)
    response=requests.get(base+"/api/dashboards/uid/cryptopulse-main",auth=grafana_auth(),timeout=20)
    response.raise_for_status()
    dashboard=response.json()["dashboard"]
    checks=[]
    current=int(time.time()*1000)
    for panel in dashboard["panels"]:
        if panel.get("datasource",{}).get("type")!="postgres" or not panel.get("targets"):
            continue
        target=dict(panel["targets"][0])
        target.update({"intervalMs":15000,"maxDataPoints":1000})
        query_response=requests.post(base+"/api/ds/query",auth=grafana_auth(),timeout=30,
            json={"queries":[target],"from":str(current-3*3600000),"to":str(current)})
        assert query_response.status_code==200,(panel["title"],query_response.text)
        query_response.raise_for_status()
        result=query_response.json()
        assert "A" in result.get("results",{}),(panel["title"],result)
        item=result.get("results",{}).get("A",{})
        checks.append(validate_panel_result(panel,item))
    batch=db.query("SELECT count(*) days,sum(candles) rows FROM batch_kpis")[0]
    assert batch["rows"]>1_000_000
    return status("S11",grafana_dashboard="cryptopulse-main",sql_panels_checked=checks,
        batch_rows=int(batch["rows"]),model_metrics=db.query("SELECT count(*) n FROM model_metrics")[0]["n"])

def verify_final():
    sessions={}
    # S07-S11 are executed below; an earlier failed live check must be retryable.
    for number in range(3,7):
        key=f"S{number:02d}"
        path=STATE/(key.lower()+".json")
        assert path.exists(),f"Falta {key}"
        item=json.loads(path.read_text())
        assert item["status"]=="PASS"
        sessions[key]=item["status"]
    ingestion=verify_market_ingestion()
    streaming=verify_streaming()
    inference=verify_inference()
    observability=verify_observability()
    bi=verify_bi()
    for number in range(7,12):
        sessions[f"S{number:02d}"]=json.loads((STATE/f"s{number:02d}.json").read_text())["status"]
    duplicates=db.query("SELECT count(*) n FROM (SELECT symbol,open_ms FROM market_candles GROUP BY symbol,open_ms HAVING count(*)>1) x")[0]["n"]
    assert duplicates==0
    latest=db.query("SELECT symbol,max(close_ms) latest_ms FROM market_candles GROUP BY symbol")
    assert all(time.time()*1000-row["latest_ms"]<180000 for row in latest),latest
    integrations=json.loads((STATE/"integration_tests.json").read_text())
    assert integrations["status"]=="PASS"
    version=json.loads((ML/"model_registry.json").read_text())["model_version"]
    assert integrations.get("model_version")==version,"Las pruebas de integracion corresponden a otra version del modelo"
    return status("S12",unit=2,sessions=sessions,duplicate_candles=duplicates,
        fresh_candles=latest,integration_tests=integrations,
        integration_evidence="Resultado guardado y versionado; para ejecutar de nuevo usa scripts/pruebas_integracion.ps1",
        health=inference["health"],
        live_evaluated=db.query("SELECT dimension,count(*) n FROM predictions WHERE actual IS NOT NULL AND source_mode='live' GROUP BY dimension"),
        replay_evaluated=db.query("SELECT dimension,count(*) n FROM predictions WHERE actual IS NOT NULL AND source_mode='replay' GROUP BY dimension"),
        live_future_results="Se incorporan al madurar los horizontes de 5,15 y30 minutos; no se adelanta la verdad futura.")

if __name__=="__main__":
    import sys
    selected=sys.argv[1] if len(sys.argv)>1 else "12"
    functions={"6":kafka_smoke,"7":verify_market_ingestion,"8":verify_streaming,"9":verify_observability,
               "10":verify_inference,"11":verify_bi,"12":verify_final}
    try:
        result=functions[selected]()
    except Exception as error:
        failed={"session":f"S{int(selected):02d}","status":"FAIL","verified_at_ms":int(time.time()*1000),
                "error":str(error)}
        save_json(STATE/f"s{int(selected):02d}.json",failed)
        raise
    print(json.dumps(result,indent=2,default=str))
