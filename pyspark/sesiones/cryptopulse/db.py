"""Persistencia durable e idempotente por clave de negocio."""
import os
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values, Json

# Se aplica también sobre volúmenes existentes: init.sql solo corre al crear PG.
PRODUCT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS market_signals(
 dimension text NOT NULL, symbol text NOT NULL, as_of_ms bigint NOT NULL,
 kind text NOT NULL CHECK(kind IN ('observed','forecast')), model_version text NOT NULL,
 value double precision, threshold double precision, triggered boolean NOT NULL,
 severity text NOT NULL, title text NOT NULL, explanation text NOT NULL, action text NOT NULL,
 unit text NOT NULL, method text NOT NULL, source_mode text NOT NULL, expires_ms bigint NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(dimension,symbol,as_of_ms,kind,model_version));
CREATE INDEX IF NOT EXISTS market_signals_time ON market_signals(as_of_ms DESC);
CREATE TABLE IF NOT EXISTS model_versions(
 model_version text PRIMARY KEY, registry jsonb NOT NULL,
 registered_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS runtime_configuration(key text PRIMARY KEY,value text NOT NULL);
ALTER TABLE predictions ADD COLUMN IF NOT EXISTS evaluation_status text NOT NULL DEFAULT 'pending';
ALTER TABLE predictions ADD COLUMN IF NOT EXISTS evaluation_reason text;
ALTER TABLE predictions ADD COLUMN IF NOT EXISTS evaluation_checked_at timestamptz;
CREATE INDEX IF NOT EXISTS predictions_pending_evaluation ON predictions(evaluation_checked_at,as_of_ms) WHERE actual IS NULL;
UPDATE predictions SET evaluation_status='evaluated' WHERE actual IS NOT NULL AND evaluation_status<>'evaluated';
GRANT SELECT ON market_signals,model_versions,runtime_configuration TO grafana_reader;
"""


def ensure_product_schema(registry=None):
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(PRODUCT_SCHEMA_SQL)
        if registry:
            cursor.execute("INSERT INTO model_versions(model_version,registry) VALUES(%s,%s) ON CONFLICT DO NOTHING",
                           (registry["model_version"], Json(registry)))
            cursor.execute("INSERT INTO runtime_configuration(key,value) VALUES('active_model_version',%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
                           (registry["model_version"],))

@contextmanager
def connect():
    connection = psycopg2.connect(host=os.environ.get("PGHOST","postgres"),
        dbname=os.environ.get("PGDATABASE","cryptopulse"),
        user=os.environ.get("PGUSER","cryptopulse"),password=os.environ.get("PGPASSWORD","local-cryptopulse"),
        connect_timeout=10)
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

def query(sql, params=None):
    with connect() as connection, connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql, params)
        return list(cursor.fetchall())

def insert_candles(events):
    if not events:
        return 0
    values = [(e["symbol"],e["open_ms"],e["close_ms"],e["open"],e["high"],e["low"],e["close"],
        e["volume"],e["quote_asset_volume"],e["number_of_trades"],e["source_mode"],e["transport"],e["published_ms"]) for e in events]
    with connect() as connection, connection.cursor() as cursor:
        result = execute_values(cursor, """INSERT INTO market_candles(symbol,open_ms,close_ms,open,high,low,close,volume,
            quote_asset_volume,number_of_trades,source_mode,transport,published_ms) VALUES %s
            ON CONFLICT(symbol,open_ms) DO NOTHING RETURNING open_ms""", values, fetch=True)
        return len(result)

def insert_predictions(values):
    if not values:
        return
    with connect() as connection, connection.cursor() as cursor:
        execute_values(cursor, """INSERT INTO predictions(dimension,symbol,as_of_ms,horizon_minutes,forecast,
            baseline,observed,model_version,method,source_mode,alert) VALUES %s ON CONFLICT DO NOTHING""", values)

def recent_candles(limit=180):
    # No mezcla replays de pruebas con la entrada actual de inferencia.
    return query("""SELECT * FROM (SELECT *,row_number() OVER(PARTITION BY symbol ORDER BY open_ms DESC) rn
                    FROM market_candles WHERE source_mode<>'replay') q WHERE rn<=%s ORDER BY symbol,open_ms""",(limit,))

def insert_kpis(values):
    if not values:
        return
    with connect() as connection,connection.cursor() as cursor:
        execute_values(cursor,"""INSERT INTO market_kpis(symbol,as_of_ms,metrics) VALUES %s
            ON CONFLICT(symbol,as_of_ms) DO UPDATE SET metrics=EXCLUDED.metrics""",
            [(symbol,asof,Json(metrics)) for symbol,asof,metrics in values])


def insert_signals(values):
    if not values:
        return
    columns = ("dimension", "symbol", "as_of_ms", "kind", "model_version", "value", "threshold",
               "triggered", "severity", "title", "explanation", "action", "unit", "method", "source_mode", "expires_ms")
    with connect() as connection, connection.cursor() as cursor:
        execute_values(cursor, "INSERT INTO market_signals(" + ",".join(columns) + ") VALUES %s "
            "ON CONFLICT(dimension,symbol,as_of_ms,kind,model_version) DO UPDATE SET "
            "value=EXCLUDED.value,threshold=EXCLUDED.threshold,triggered=EXCLUDED.triggered,"
            "severity=EXCLUDED.severity,title=EXCLUDED.title,explanation=EXCLUDED.explanation,"
            "action=EXCLUDED.action,expires_ms=EXCLUDED.expires_ms",
            [tuple(row.get(k) for k in columns) for row in values])
