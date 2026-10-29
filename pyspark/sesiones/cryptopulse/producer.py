"""S06/S07: productor de velas reales por WebSocket con recuperacion REST."""
import json
import os
import signal
import time
import traceback
import argparse
from collections import OrderedDict
import requests
import websocket
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from .contract import SYMBOLS, make_event, from_websocket, validate
from .core import STATE, save_json, status

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPICS = {s: "market.klines." + s.lower() for s in SYMBOLS}
REST = "https://data-api.binance.vision"
WS = "wss://data-stream.binance.vision/stream?streams=btcusdt@kline_1m/solusdt@kline_1m"
running = True
MINUTE = 60000

def fetch_candles(session, symbol, start, end):
    response = session.get(REST + "/api/v3/klines", params={"symbol":symbol,"interval":"1m",
        "limit":1000,"startTime":start,"endTime":end}, timeout=25)
    response.raise_for_status()
    candles = response.json()
    if not isinstance(candles,list) or any(not isinstance(c,list) or len(c)<9 for c in candles):
        raise ValueError("Respuesta REST de velas invalida")
    return sorted(candles,key=lambda c:int(c[0]))

class ReconciledPublisher:
    """El cursor REST avanza tras consultar un rango y confirmar sus publicaciones.

    WS no avanza ese cursor. Una vela intermedia anterior al maximo recibido sigue
    siendo publicable; Kafka y PostgreSQL conservan la misma clave de negocio.
    """
    def __init__(self, producer, session, previous=None, persist=None, clock=None):
        previous=previous or {}
        self.producer,self.session=producer,session
        self.persist=persist or (lambda value:None)
        self.clock=clock or (lambda:int(time.time()*1000))
        self.seen=OrderedDict()
        self.last_save=0
        self.info={"connected":False,"websocket_messages":0,
            **{k:previous.get(k,0) for k in ("published","live_published","backfill_published")},
            "reconnections":0,"rejected":0,"last_open_ms":dict(previous.get("last_open_ms",{})),
            "rest_reconciled_open_ms":dict(previous.get("rest_reconciled_open_ms",{})),
            "last_closed_event_ms":previous.get("last_closed_event_ms",0),
            "last_transport":None,"source_gaps":{}}

    def heartbeat(self,force=False):
        current=self.clock()
        self.info["heartbeat_ms"]=current
        if force or current-self.last_save>=1000:
            self.persist(self.info)
            self.last_save=current

    def publish(self,event):
        validate(event)
        key=event["event_id"]
        if key in self.seen:
            return False
        self.producer.send(TOPICS[event["symbol"]],key=event["symbol"],value=event).get(timeout=30)
        self.seen[key]=None
        if len(self.seen)>12000:
            self.seen.popitem(last=False)
        symbol=event["symbol"]
        self.info["last_open_ms"][symbol]=max(event["open_ms"],self.info["last_open_ms"].get(symbol,-1))
        self.info["published"]+=1
        self.info[event["source_mode"]+"_published"]+=1
        self.info["last_transport"]=event["transport"]
        self.info["last_closed_event_ms"]=max(event["close_ms"],self.info["last_closed_event_ms"])
        self.heartbeat()
        print(json.dumps({"published":key,"mode":event["source_mode"],"transport":event["transport"]}),flush=True)
        return True

    def reconcile(self,server_time=None,symbols=SYMBOLS):
        if server_time is None:
            response=self.session.get(REST+"/api/v3/time",timeout=20)
            response.raise_for_status()
            server_time=int(response.json()["serverTime"])
        last_closed_open=(server_time//MINUTE-1)*MINUTE
        for symbol in symbols:
            cursor=self.info["rest_reconciled_open_ms"].get(symbol)
            if cursor is None:
                # Old states can hide a lost minute below last_open_ms: recheck 24h.
                high=self.info["last_open_ms"].get(symbol)
                cursor=high-1440*MINUTE if high is not None else last_closed_open-180*MINUTE
            # Revisit three hours, also after REST has caught up, to repair late data.
            start=max(0,min(cursor+MINUTE,last_closed_open-179*MINUTE))
            unavailable=[]
            while running and start<=last_closed_open:
                end_open=min(start+999*MINUTE,last_closed_open)
                candles=fetch_candles(self.session,symbol,start,end_open+MINUTE-1)
                found=set()
                for candle in candles:
                    candle_open,candle_close=int(candle[0]),int(candle[6])
                    if not start<=candle_open<=end_open or candle_close>=server_time:
                        raise ValueError("Vela REST fuera del rango cerrado solicitado")
                    mode="live" if candle_close>=server_time-120000 else "backfill"
                    self.publish(make_event(symbol,candle,mode,"rest"))
                    found.add(candle_open)
                unavailable.extend(t for t in range(start,end_open+1,MINUTE) if t not in found)
                # A failed ACK never advances the reconciliation cursor for this page.
                self.info["rest_reconciled_open_ms"][symbol]=max(cursor,end_open)
                self.heartbeat(force=True)
                start=end_open+MINUTE
            self.info["source_gaps"][symbol]={"count":len(unavailable),"open_ms":unavailable[-180:]}
        self.info["last_rest_sync_ms"]=self.clock()
        self.info.pop("last_error",None)
        self.heartbeat(force=True)

def repair_gaps(hours=72):
    """Republicar minutos ausentes desde Binance sin alterar el estado del productor."""
    from . import db
    if not 1<=hours<=744:
        raise ValueError("--hours debe estar entre 1 y 744")
    since=int(time.time()*1000)-hours*3600000
    gaps=db.query("""WITH timeline AS (
        SELECT symbol,open_ms,lag(open_ms) OVER(PARTITION BY symbol ORDER BY open_ms) previous
        FROM market_candles WHERE open_ms>=%s)
        SELECT symbol,previous+60000 AS start_ms,open_ms-60000 AS end_ms FROM timeline
        WHERE open_ms-previous>60000 ORDER BY symbol,open_ms""",(since,))
    producer=KafkaProducer(bootstrap_servers=BOOTSTRAP,acks="all",retries=10,
        key_serializer=lambda s:s.encode(),value_serializer=lambda value:json.dumps(value).encode(),
        client_id="cryptopulse-repair",enable_idempotence=True)
    session=requests.Session()
    recovered,unavailable=0,[]
    try:
        for gap in gaps:
            start=gap["start_ms"]
            while start<=gap["end_ms"]:
                end=min(start+999*MINUTE,gap["end_ms"])
                found=set()
                for candle in fetch_candles(session,gap["symbol"],start,end+MINUTE-1):
                    if not start<=int(candle[0])<=end:
                        raise ValueError("Vela de reparacion fuera de rango")
                    event=make_event(gap["symbol"],candle,"backfill","rest")
                    producer.send(TOPICS[gap["symbol"]],key=gap["symbol"],value=event).get(timeout=30)
                    found.add(int(candle[0]));recovered+=1
                unavailable.extend({"symbol":gap["symbol"],"open_ms":t}
                    for t in range(start,end+1,MINUTE) if t not in found)
                start=end+MINUTE
        producer.flush()
    finally:
        producer.close();session.close()
    result={"status":"PASS" if not unavailable else "DEGRADED","hours":hours,
        "gap_ranges":len(gaps),"republished_real_candles":recovered,"unavailable":unavailable}
    save_json(STATE/"ingestion_repair.json",result)
    return result
def stop(*_):
    global running
    running = False

def ensure_topics():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="cryptopulse-admin")
    present = set(admin.list_topics())
    desired = [*TOPICS.values(), "market.deadletter", "cryptopulse.integration"]
    missing = [NewTopic(name, num_partitions=3, replication_factor=1) for name in desired if name not in present]
    if missing:
        try:
            admin.create_topics(missing)
        except TopicAlreadyExistsError:
            pass
    admin.close()

def run():
    signal.signal(signal.SIGTERM, stop)
    ensure_topics()
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", retries=10,
        key_serializer=lambda s:s.encode(), value_serializer=lambda value:json.dumps(value).encode(),
        client_id="cryptopulse-market", enable_idempotence=True)
    session = requests.Session()
    state_file = STATE / "producer_status.json"
    previous = json.loads(state_file.read_text(encoding="utf-8")) if state_file.exists() else {}
    publisher=ReconciledPublisher(producer,session,previous,lambda value:save_json(state_file,value))
    info=publisher.info
    heartbeat=lambda:publisher.heartbeat(force=True)
    publish=publisher.publish
    catch_up=publisher.reconcile
    backoff = 1
    while running:
        connection = None
        try:
            catch_up()
            connection = websocket.create_connection(WS,timeout=5)
            info["connected"] = True
            info["last_websocket_ms"] = int(time.time()*1000)
            heartbeat()
            next_sync, next_heartbeat = time.monotonic()+60, time.monotonic()+10
            backoff = 1
            while running:
                try:
                    payload = json.loads(connection.recv())
                    info["websocket_messages"] += 1
                    info["last_websocket_ms"] = int(time.time()*1000)
                    event = from_websocket(payload)
                    if event is not None:
                        publish(event)
                except websocket.WebSocketTimeoutException:
                    pass
                except (ValueError, KeyError, TypeError, ArithmeticError) as error:
                    info["rejected"] += 1
                    print("Mensaje rechazado:",repr(error),flush=True)
                if time.monotonic() >= next_sync:
                    catch_up()
                    next_sync = time.monotonic()+60
                if time.monotonic() >= next_heartbeat:
                    heartbeat()
                    next_heartbeat = time.monotonic()+10
            if connection:
                connection.close()
        except Exception as error:
            info["connected"] = False
            info["reconnections"] += 1
            info["last_error"] = type(error).__name__ + ": " + str(error)
            heartbeat()
            traceback.print_exc()
            if running:
                time.sleep(backoff)
            backoff = min(30, backoff*2)
        finally:
            if connection:
                connection.close()
    producer.flush()
    producer.close()
    session.close()

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repair-gaps",action="store_true")
    parser.add_argument("--hours",type=int,default=72)
    args=parser.parse_args()
    if args.repair_gaps:
        print(json.dumps(repair_gaps(args.hours),indent=2))
    else:
        run()
