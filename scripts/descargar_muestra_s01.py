"""Descarga real de Binance Spot 1m, SHA256 y fechas UTC. Solo biblioteca estandar."""
import argparse
import csv
import hashlib
import io
import json
import os
import urllib.request
import zipfile
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.environ.get("CRYPTOPULSE_DATA_DIR", str(ROOT / "pyspark/sesiones/s01-arquitectura/data")))
SYMBOLS = ("BTCUSDT", "SOLUSDT")
FIELDS = ["symbol", "open_time", "open", "high", "low", "close", "volume",
          "close_time", "quote_asset_volume", "number_of_trades", "source", "is_closed"]

def fetch(url):
    request = urllib.request.Request(url, headers={"User-Agent": "CryptoPulse-S01/1.0"})
    with urllib.request.urlopen(request, timeout=90) as response:
        return response.read()

def utc_timestamp(value, unit):
    divisor = 1_000_000 if unit == "us" else 1_000
    seconds, fraction = divmod(int(value), divisor)
    micros = fraction if unit == "us" else fraction * 1_000
    return datetime.fromtimestamp(seconds, timezone.utc).replace(microsecond=micros).isoformat(timespec="microseconds").replace("+00:00", "Z")

def prepare(day):
    parsed_day = date.fromisoformat(day)
    unit = "us" if parsed_day >= date(2025, 1, 1) else "ms"
    DATA.mkdir(parents=True, exist_ok=True)
    raw = DATA / "raw"
    raw.mkdir(exist_ok=True)
    combined, records = [], []
    for symbol in SYMBOLS:
        name = f"{symbol}-1m-{day}.zip"
        url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/1m/{name}"
        payload = fetch(url)
        checksum_text = fetch(url + ".CHECKSUM").decode("utf-8")
        expected = checksum_text.split()[0].lower()
        actual = hashlib.sha256(payload).hexdigest()
        if expected != actual:
            raise ValueError(f"SHA256 no coincide: {name}")
        (raw / name).write_bytes(payload)
        (raw / (name + ".CHECKSUM")).write_text(checksum_text, encoding="utf-8")
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
            if len(names) != 1:
                raise ValueError(f"Numero inesperado de CSV: {name}")
            lines = list(csv.reader(io.StringIO(archive.read(names[0]).decode("utf-8-sig"))))
        if not lines:
            raise ValueError(f"Archivo vacio: {name}")
        seen = set()
        for row in lines:
            if len(row) != 12:
                raise ValueError(f"Se esperaban 12 campos: {name}")
            start, end = utc_timestamp(row[0], unit), utc_timestamp(row[6], unit)
            if start in seen or start[:10] != day:
                raise ValueError(f"Vela duplicada o fecha inesperada: {name}: {start}")
            seen.add(start)
            o, h, l, c = [Decimal(v) for v in row[1:5]]
            if not (0 < l <= min(o, c) <= max(o, c) <= h):
                raise ValueError(f"OHLC incoherente: {name}: {start}")
            if Decimal(row[5]) < 0 or Decimal(row[7]) < 0 or int(row[8]) < 0 or start >= end:
                raise ValueError(f"Volumen, operaciones o intervalo invalido: {start}")
            combined.append([symbol, start, *row[1:6], end, row[7], row[8],
                             "binance_public_data_spot", "true"])
        records.append({"symbol": symbol, "url": url, "checksum_url": url + ".CHECKSUM",
                        "sha256_zip": actual, "checksum_verified": True,
                        "rows": len(lines), "timestamp_unit_raw": unit,
                        "first_open_time_utc": utc_timestamp(lines[0][0], unit),
                        "last_open_time_utc": utc_timestamp(lines[-1][0], unit)})
    combined.sort(key=lambda row: (row[1], row[0]))
    path = DATA / "cryptopulse_klines_muestra.csv"
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(FIELDS)
        writer.writerows(combined)
    manifest = {"project": "CryptoPulse", "session": "S01", "market": "spot", "interval": "1m",
                "date_utc": day, "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
                "synthetic": False, "rows_total": len(combined), "archives": records,
                "csv_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "note": "Muestra real para verificar Spark; no es el historico de entrenamiento 2022+ ni un stream en vivo."}
    (DATA / "provenance.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fecha", default="2025-01-01", help="Dia UTC cerrado, AAAA-MM-DD")
    prepare(parser.parse_args().fecha)
