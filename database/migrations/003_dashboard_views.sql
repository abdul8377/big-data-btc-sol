CREATE INDEX IF NOT EXISTS market_symbol_recent ON market_candles(symbol,open_ms DESC);
CREATE INDEX IF NOT EXISTS predictions_live_recent ON predictions(dimension,symbol,model_version,as_of_ms DESC) WHERE source_mode='live';
CREATE OR REPLACE VIEW cp_asset_health AS
WITH assets(symbol) AS (VALUES ('BTCUSDT'),('SOLUSDT')),
recent AS (
 SELECT a.symbol,c.* FROM assets a CROSS JOIN LATERAL
 (SELECT open_ms,close_ms FROM market_candles WHERE symbol=a.symbol AND source_mode<>'replay' ORDER BY open_ms DESC LIMIT 61) c
), agg AS (
 SELECT symbol,count(*) context_rows,min(open_ms) first_open,max(open_ms) last_open,max(close_ms) last_close,
 greatest(0,(max(open_ms)-min(open_ms))/60000+1-count(*)) missing_minutes FROM recent GROUP BY symbol
)
SELECT a.symbol,g.context_rows,g.missing_minutes,g.last_close,
 (extract(epoch FROM now())*1000-g.last_close)/1000.0 candle_age_seconds,
 k.as_of_ms kpi_as_of_ms,(extract(epoch FROM now())*1000-k.as_of_ms)/1000.0 kpi_age_seconds,
 CASE WHEN g.last_close IS NULL THEN 'SIN DATOS' WHEN extract(epoch FROM now())*1000-g.last_close>180000 THEN 'DESACTUALIZADO'
 WHEN g.context_rows<61 THEN 'CALENTANDO' WHEN g.missing_minutes>0 THEN 'DATOS INCOMPLETOS' ELSE 'LISTO' END status
FROM assets a LEFT JOIN agg g ON g.symbol=a.symbol
LEFT JOIN LATERAL (SELECT as_of_ms FROM market_kpis WHERE symbol=a.symbol ORDER BY as_of_ms DESC LIMIT 1) k ON true;

CREATE OR REPLACE VIEW cp_signal_board AS
WITH dimensions(dimension,symbol) AS (VALUES ('volatility','BTCUSDT'),('volatility','SOLUSDT'),('crash','BTCUSDT'),
 ('crash','SOLUSDT'),('anomaly','BTCUSDT'),('anomaly','SOLUSDT'),('divergence','BTC-SOL')),
kinds(kind) AS (VALUES ('observed'),('forecast')),
version AS (SELECT value v FROM runtime_configuration WHERE key='active_model_version')
SELECT d.dimension,d.symbol,k.kind,s.as_of_ms,s.value,s.threshold,s.unit,s.triggered,s.method,s.model_version,
 CASE WHEN s.as_of_ms IS NULL THEN 'SIN EVALUAR' WHEN extract(epoch FROM now())*1000-s.as_of_ms>180000 THEN 'DESACTUALIZADO'
 WHEN s.method='baseline' AND k.kind='forecast' THEN 'REFERENCIA'
 WHEN s.severity='unavailable' THEN 'NO DISPONIBLE' WHEN s.severity='high' THEN 'ALERTA'
 WHEN s.triggered THEN 'VIGILANCIA' WHEN s.method='baseline' AND k.kind='forecast' THEN 'REFERENCIA' ELSE 'NORMAL' END state,
 CASE WHEN s.as_of_ms IS NULL THEN 'Esperando velas consecutivas y un cálculo completo.'
 WHEN extract(epoch FROM now())*1000-s.as_of_ms>180000 THEN 'La evaluación está atrasada: revisar continuidad e ingesta antes de interpretar el riesgo.'
 ELSE s.explanation END explanation,
 CASE WHEN s.as_of_ms IS NULL OR extract(epoch FROM now())*1000-s.as_of_ms>180000 THEN 'Revisar estado del producto y frescura por activo.' ELSE s.action END action,
 (extract(epoch FROM now())*1000-s.as_of_ms)/1000.0 age_seconds
FROM dimensions d CROSS JOIN kinds k CROSS JOIN version v
LEFT JOIN LATERAL (SELECT * FROM market_signals WHERE dimension=d.dimension AND symbol=d.symbol AND kind=k.kind
 AND source_mode='live' AND model_version=v.v ORDER BY as_of_ms DESC LIMIT 1) s ON true;
GRANT SELECT ON cp_asset_health,cp_signal_board TO grafana_reader;
