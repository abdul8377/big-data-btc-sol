"""Historicos mensuales Binance Spot. CSV originales y SHA256 verificables.

La descarga inicial usa 2024-12 a 2025-01; --desde y --hasta permiten ampliarla.
No necesita credenciales. Las filas no se fabrican ni se imputan.
"""
import argparse
import calendar
import hashlib
import io
import json
import os
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA = Path(os.environ.get("CRYPTOPULSE_DATA_DIR",
    str(PROJECT_ROOT / "pyspark/sesiones/s02-fundamentos/data")))
SYMBOLS = ("BTCUSDT", "SOLUSDT")

def months_between(start, end):
    first = datetime.strptime(start, "%Y-%m")
    last = datetime.strptime(end, "%Y-%m")
    if start != first.strftime("%Y-%m") or end != last.strftime("%Y-%m"):
        raise ValueError("Usa meses AAAA-MM")
    if first > last:
        raise ValueError("--desde debe ser menor o igual a --hasta")
    if (last.year, last.month) >= (datetime.now(timezone.utc).year, datetime.now(timezone.utc).month):
        raise ValueError("Usa meses UTC ya cerrados")
    year, month = first.year, first.month
    while (year, month) <= (last.year, last.month):
        yield f"{year:04d}-{month:02d}"
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)

def fetch(url):
    for attempt in range(3):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "CryptoPulse-S02/1.0"})
            with urllib.request.urlopen(request, timeout=90) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code == 404:
                raise RuntimeError(f"Archivo oficial no disponible; no se sustituye por datos simulados: {url}") from error
            if attempt == 2 or error.code < 500:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == 2:
                raise
        time.sleep(2 ** attempt)

def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()

def download(start, end, data_dir):
    months = list(months_between(start, end))
    raw_dir = data_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    entries = []
    for month in months:
        year, number = map(int, month.split("-"))
        for symbol in SYMBOLS:
            base = f"{symbol}-1m-{month}"
            url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/1m/{base}.zip"
            expected_text = fetch(url + ".CHECKSUM").decode("utf-8")
            expected = expected_text.split()[0].lower()
            archive_path = raw_dir / (base + ".zip")
            cached = archive_path.exists() and sha256_file(archive_path) == expected
            if not cached:
                payload = fetch(url)
                if hashlib.sha256(payload).hexdigest() != expected:
                    raise ValueError(f"SHA256 incorrecto: {url}")
                temporary = archive_path.with_suffix(".zip.part")
                temporary.write_bytes(payload)
                temporary.replace(archive_path)
            (raw_dir / (base + ".zip.CHECKSUM")).write_text(expected_text, encoding="utf-8")
            csv_path = raw_dir / (base + ".csv")
            with zipfile.ZipFile(archive_path) as archive:
                names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                if len(names) != 1:
                    raise ValueError(f"Se esperaba un CSV en {archive_path.name}")
                # Leer el miembro evita extraer rutas arbitrarias del ZIP.
                content = archive.read(names[0])
            rows = len(content.splitlines())
            if not rows:
                raise ValueError(f"CSV vacio: {base}")
            temp_csv = csv_path.with_suffix(".csv.part")
            temp_csv.write_bytes(content)
            temp_csv.replace(csv_path)
            record = {
                "symbol": symbol, "month": month, "url": url,
                "checksum_url": url + ".CHECKSUM", "checksum_verified": True,
                "sha256_zip": expected, "sha256_csv": hashlib.sha256(content).hexdigest(),
                "csv_relative_path": f"raw/{base}.csv", "rows": rows,
                "expected_rows_calendar": calendar.monthrange(year, number)[1] * 1440,
                "timestamp_unit": "us" if month >= "2025-01" else "ms",
            }
            entries.append(record)
            print(f"{base}: {rows:,} filas, SHA256 OK" + (" (cache)" if cached else ""), flush=True)
    manifest = {
        "project": "CryptoPulse", "session": "S02", "market": "spot",
        "interval": "1m", "symbols": list(SYMBOLS), "month_from": start,
        "month_to": end, "synthetic": False,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows_total": sum(item["rows"] for item in entries), "archives": entries,
        "scope": "Intervalo descargado para S02; ampliar con --desde/--hasta para el historico 2022+.",
    }
    temp_manifest = data_dir / "provenance.json.part"
    temp_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp_manifest.replace(data_dir / "provenance.json")
    print(f"Listo: {len(entries)} archivos, {manifest['rows_total']:,} velas reales.", flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--desde", default="2024-12")
    parser.add_argument("--hasta", default="2025-01")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    args = parser.parse_args()
    download(args.desde, args.hasta, args.data_dir)
