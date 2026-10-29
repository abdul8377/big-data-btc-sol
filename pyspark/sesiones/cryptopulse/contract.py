"""Contrato v1 de velas cerradas y validacion de eventos de mercado."""
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

SYMBOLS = ("BTCUSDT", "SOLUSDT")
def validate(event):
    if not isinstance(event, dict):
        raise ValueError("event_must_be_object")
    if type(event.get("schema_version")) is not int or event.get("schema_version") != 1 or event.get("event_type") != "market.candle.closed":
        raise ValueError("unsupported_contract")
    if event.get("symbol") not in SYMBOLS or event.get("interval") != "1m" or event.get("is_closed") is not True:
        raise ValueError("symbol_interval_or_unclosed")
    if event.get("source_mode") not in ("live", "backfill", "replay"):
        raise ValueError("unknown_source_mode")
    if event.get("synthetic") is not False or event.get("source") != "binance_spot":
        raise ValueError("untrusted_source")
    if event.get("transport") not in ("websocket","rest","replay") or type(event.get("published_ms")) is not int or not 0 <= event["published_ms"] < 2**63:
        raise ValueError("missing_transport_or_publish_time")
    if type(event.get("number_of_trades")) is not int or not 0 <= event["number_of_trades"] < 2**63:
        raise ValueError("invalid_trade_count")
    start, end = event["open_ms"], event["close_ms"]
    if type(start) is not int or type(end) is not int or not 0 <= start < end < 2**63 or start % 60000 or end - start != 59999:
        raise ValueError("invalid_candle_interval")
    if end > int(time.time()*1000) + 10000:
        raise ValueError("future_candle")
    numbers = {}
    for key in ("open", "high", "low", "close", "volume", "quote_asset_volume"):
        try:
            number = Decimal(str(event[key]))
            if not number.is_finite():
                raise ValueError("non_finite_market_value")
            if number.copy_abs() >= Decimal("1e20") or number.quantize(Decimal("0.00000001")) != number:
                raise ValueError("market_value_outside_numeric_28_8")
            numbers[key] = number
        except (InvalidOperation, TypeError, OverflowError) as error:
            raise ValueError("invalid_decimal_market_value") from error
    o,h,l,c = [numbers[k] for k in ("open","high","low","close")]
    if not (0 < l <= min(o,c) <= max(o,c) <= h):
        raise ValueError("invalid_ohlc")
    if numbers["volume"] < 0 or numbers["quote_asset_volume"] < 0 or event["number_of_trades"] < 0:
        raise ValueError("negative_activity")
    if event.get("event_id") != f"{event['symbol']}:1m:{start}":
        raise ValueError("invalid_event_id")
    return event

def make_event(symbol, candle, mode, transport):
    start, end = int(candle[0]), int(candle[6])
    event = {"schema_version":1,"event_type":"market.candle.closed","event_id":f"{symbol}:1m:{start}",
        "symbol":symbol,"interval":"1m","open_ms":start,"close_ms":end,
        "event_time":datetime.fromtimestamp(end / 1000, timezone.utc).isoformat(),
        "open":str(candle[1]),"high":str(candle[2]),"low":str(candle[3]),"close":str(candle[4]),
        "volume":str(candle[5]),"quote_asset_volume":str(candle[7]),"number_of_trades":int(candle[8]),
        "is_closed":True,"source":"binance_spot","source_mode":mode,"transport":transport,
        "published_ms":int(time.time()*1000),"synthetic":False}
    return validate(event)

def from_websocket(message):
    payload = message.get("data", message)
    if payload.get("e") != "kline" or not payload.get("k",{}).get("x"):
        return None
    k = payload["k"]
    return make_event(k["s"],[k["t"],k["o"],k["h"],k["l"],k["c"],k["v"],k["T"],k["q"],k["n"]],"live","websocket")
