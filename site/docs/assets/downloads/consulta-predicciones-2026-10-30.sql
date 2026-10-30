SELECT DISTINCT ON (dimension, symbol)
  dimension, symbol,
  to_timestamp(as_of_ms / 1000.0) AS cierre,
  round(forecast::numeric, 6) AS pronostico,
  method AS metodo, evaluation_status AS evaluacion
FROM predictions
WHERE source_mode = 'live'
  AND model_version = (
    SELECT value FROM runtime_configuration
    WHERE key = 'active_model_version'
  )
ORDER BY dimension, symbol, as_of_ms DESC;