CREATE TABLE market_candles(
 symbol text NOT NULL, open_ms bigint NOT NULL, close_ms bigint NOT NULL,
 open numeric(28,8) NOT NULL, high numeric(28,8) NOT NULL, low numeric(28,8) NOT NULL,
 close numeric(28,8) NOT NULL, volume numeric(28,8) NOT NULL,
 quote_asset_volume numeric(28,8) NOT NULL, number_of_trades bigint NOT NULL,
 source_mode text NOT NULL, transport text NOT NULL, published_ms bigint NOT NULL,
 received_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(symbol,open_ms));
CREATE INDEX market_time ON market_candles(open_ms DESC);
CREATE TABLE predictions(
 dimension text NOT NULL, symbol text NOT NULL, as_of_ms bigint NOT NULL,
 horizon_minutes int NOT NULL, forecast double precision NOT NULL, baseline double precision NOT NULL,
 observed double precision, model_version text NOT NULL, method text NOT NULL,
 source_mode text NOT NULL, alert boolean NOT NULL DEFAULT false,
 issued_at timestamptz NOT NULL DEFAULT now(),
 actual double precision, evaluated_at timestamptz,
 PRIMARY KEY(dimension,symbol,as_of_ms,model_version));
CREATE INDEX predictions_time ON predictions(as_of_ms DESC);
CREATE TABLE stream_windows(
 kind text NOT NULL, symbol text NOT NULL, window_start timestamptz NOT NULL,
 window_end timestamptz NOT NULL, candles bigint NOT NULL,
 quote_volume_usdt numeric(30,8), avg_close_usdt double precision, final boolean NOT NULL,
 PRIMARY KEY(kind,symbol,window_start));
CREATE TABLE market_kpis(symbol text NOT NULL, as_of_ms bigint NOT NULL, metrics jsonb NOT NULL, PRIMARY KEY(symbol,as_of_ms));
CREATE TABLE rejected_events(
 event_hash text PRIMARY KEY, reason text NOT NULL, payload jsonb NOT NULL, received_at timestamptz DEFAULT now());
CREATE TABLE model_metrics(
 dimension text NOT NULL, split text NOT NULL, method text NOT NULL,
 model_version text NOT NULL, metrics jsonb NOT NULL,
 PRIMARY KEY(dimension,split,method,model_version));
CREATE TABLE batch_kpis(
 symbol text NOT NULL, date_utc date NOT NULL, candles bigint NOT NULL,
 quote_volume_usdt numeric(30,8), trades bigint, PRIMARY KEY(symbol,date_utc));
CREATE TABLE stream_batches(
 query_name text NOT NULL, batch_id bigint NOT NULL, rows_count bigint NOT NULL,
 duration_ms double precision, payload jsonb, updated_at timestamptz DEFAULT now(),
 PRIMARY KEY(query_name,batch_id));
CREATE USER grafana_reader WITH PASSWORD 'local-grafana-read';
GRANT CONNECT ON DATABASE cryptopulse TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;
