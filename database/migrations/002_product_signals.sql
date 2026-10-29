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
