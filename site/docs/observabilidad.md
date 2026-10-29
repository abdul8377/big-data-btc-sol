# Prometheus, API y salud técnica

## API de producto, salud y observabilidad técnica

### Qué hace el exportador

`cryptopulse.exporter` ejecuta un `ThreadingHTTPServer` Python en el puerto 8000 del contenedor. Lee los estados JSON compartidos, consulta PostgreSQL y, para métricas de offsets, consulta Kafka. Publica respuestas JSON y formato Prometheus. No necesita montar el socket Docker: evalúa el producto a través de sus datos y procesos, no a partir de una lista de contenedores.

No existe una aplicación frontend adicional entre el exportador y Grafana. La UI principal es el dashboard; la API permite consultar de forma estructurada lo mismo que las verificaciones necesitan comprobar. La API no publica métodos para comprar/vender ni endpoints de modificación de modelos.

### Endpoints y significado de sus respuestas

| Ruta local | Respuesta | Uso correcto |
|---|---|---|
| `http://localhost:48000/api/status` | Productor, stream, velas, predicciones, salud, señales y resultados guardados de sesiones | Diagnóstico completo. HTTP 200 por sí solo no significa producto listo: revisar `health.ready`. |
| `http://localhost:48000/api/signals` | Estado conjunto, mensaje, catorce evaluaciones y cruces activos | Consumo estructurado de señales y sus explicaciones. |
| `http://localhost:48000/health` | Política de salud y motivos | HTTP 200 si está listo; 503 si está degradado. |
| `http://localhost:48000/ready` | Mismo control integral | Preparación del producto para ser interpretado. |
| `http://localhost:48000/health/live` o `/live` | `status: ok` | Disponibilidad del proceso HTTP, sin certificar datos o modelos. |
| `http://localhost:48000/metrics` | Familias de métricas Prometheus | Scraping automático y diagnóstico técnico. |

Ante una excepción de base u otro componente, el manejador responde 503 con error. Las fechas SQL y Decimal se serializan con conversión a texto cuando corresponde; un consumidor debe distinguir strings de representación y números JSON, y no suponer que toda columna es un float.

Los resultados `sessions` incluidos en `/api/status` son evidencias previamente guardadas. Un PASS de una sesión anterior no sustituye la salud de este instante. A su vez, el contador de alertas de mercado no determina si el pipeline está técnicamente sano.

### Política completa de salud integral

`build_health()` calcula los motivos y no se limita a preguntar si PostgreSQL acepta conexiones:

1. El heartbeat del stream debe existir, tener como máximo 60 segundos y no estar más de 10 segundos en el futuro.
2. La inferencia debe estar habilitada.
3. Debe existir una versión de registro y coincidir con la versión anunciada por el stream.
4. El heartbeat del productor debe existir, tener como máximo 60 segundos y no estar más de 10 segundos en el futuro.
5. Para cada activo se toman las últimas 61 velas únicas no replay. Deben existir 61, sin saltos de minuto entre ellas, con cierre reciente de como máximo 180 segundos y sin adelantarse más de 10 segundos.
6. Deben existir las siete series live de la versión activa. Cada una debe tener ancla con edad de como máximo 180 segundos, sin adelantarse más de 10 segundos y con retraso respecto de los datos relevantes no superior a dos minutos. Para divergencia se usa el menor último cierre de ambos activos como referencia.
7. Los activos usados por cada predicción deben tener contexto listo.

Si hay cualquier motivo, `ready=false`, `status='degraded'`; sin motivos, `ready=true`. Los códigos permiten localizar `stream_stale`, `inference_disabled`, `model_mismatch`, `producer_stale`, problemas por activo y problemas por serie. Se incluyen edad, contexto, huecos y backlog maduro para que el diagnóstico pueda explicarse.

Este estado no incorpora como requisitos adicionales un volumen de negociación alto, ausencia de alertas de mercado o un modelo con determinada precisión. Tampoco exige `connected=true` de WebSocket si REST y el productor siguen proporcionando datos recientes. La conexión tiene su propia métrica/regla. El backlog de evaluación madura se informa, pero su tamaño no vuelve falso automáticamente el estado integral actual. Un `ready=true` certifica las condiciones implementadas, no todas las propiedades posibles de un sistema financiero.

La vista SQL `cp_asset_health` se centra en velas y KPI; no revisa productor, driver o versión. El panel integral usa la métrica de `build_health` para incluir esos controles. El contador 7/7 es otro control parcial: no debe interpretarse aisladamente como una prueba de contexto completo.

## Prometheus: qué mide, cómo lo guarda y para qué sirve

### Recogida y fuentes

La configuración de `observability/prometheus.yml` establece scraping cada 15 segundos y evaluación de reglas cada 15 segundos. El job `cryptopulse` consulta `exporter:8000`, con timeout de 12 segundos; `kafka` consulta `kafka-exporter:9308`; `prometheus` se consulta a sí mismo en `localhost:9090`.

Un scrape guarda muestras fechadas. Una serie se identifica por nombre y conjunto de etiquetas; por ejemplo, la edad de BTC y de SOL son series diferentes por `symbol`. Grafana puede ver su evolución histórica aunque la aplicación solo exponga el valor del momento.

Si una consulta del exportador falla antes de generar la respuesta completa, el scrape puede fallar y `up{job='cryptopulse'}` pasa a 0. Por ello, una ausencia de métricas no debe convertirse en una edad cero saludable. La regla de disponibilidad del exportador complementa las reglas sobre valores recibidos.

El proyecto no incorpora cAdvisor, node-exporter, OpenTelemetry ni un colector de logs centralizado. No hay paneles que midan automáticamente el consumo real completo de CPU/RAM/disco de Windows o el costo cloud. Los logs se consultan con Docker Compose; las métricas descritas son instrumentación específica y Kafka Exporter.

### Catálogo de las 21 familias propias

**Todas estas familias están implementadas como Gauge**, creadas nuevamente en cada captura con un registro propio. Aunque un nombre termine en `_total` o represente un acumulado, el código actual no lo declara como Counter. Las edades se expresan en segundos; las cantidades de eventos no se deben leer como segundos ni como operaciones de mercado.

#### 1. `cryptopulse_producer_connected`

Sin etiquetas. Vale 1 si el estado del productor indica conexión WebSocket y 0 en caso contrario. Sirve para ver el transporte preferido. Puede existir conexión sin velas completas, o REST puede mantener datos sin esa conexión. Leerla junto a frescura, continuidad y salud integral. No tiene por sí sola una regla de alerta sobre el valor: la regla de WebSocket usa edad de mensajes.

#### 2. `cryptopulse_producer_heartbeat_age_seconds`

Sin etiquetas. Diferencia entre ahora y `heartbeat_ms` del productor, acotada a no negativa para la métrica. Indica si el bucle publica estado. Un valor creciente puede señalar bloqueo o proceso detenido. El control integral considera más de 60 segundos; no hay una regla independiente con ese nombre en alerts.yml.

#### 3. `cryptopulse_websocket_age_seconds`

Sin etiquetas. Tiempo desde el último mensaje WebSocket recibido, incluso uno de una vela todavía abierta. Más de 90 segundos durante un minuto dispara la regla de conexión. No mide edad de la última vela archivada ni de la predicción. Al abrir conexión, el productor inicializa esa marca y luego la actualiza con mensajes.

#### 4. `cryptopulse_producer_events_total`

Sin etiquetas. Publicaciones confirmadas por Kafka según el acumulado persistido del productor. Incluye live y backfill, y puede incluir republicaciones después de reinicios. No es número de velas únicas PostgreSQL ni número de trades Binance. Un incremento muestra publicaciones; no prueba que todos sus sinks hayan terminado.

#### 5. `cryptopulse_stream_heartbeat_age_seconds`

Sin etiquetas. Edad del estado publicado por el driver Spark. Más de 60 segundos es motivo de degradación; si persiste un minuto, dispara la regla correspondiente. Un heartbeat reciente confirma que el bucle de supervisión vive, pero requiere los controles de resultados para confirmar inferencia útil.

#### 6. `cryptopulse_inference_enabled`

Sin etiquetas. 1 si el stream anuncia inferencia habilitada, 0 si no. Ayuda a distinguir un archivo de velas que avanza de un producto que además aplica modelos. No cuantifica cuántas predicciones produjo.

#### 7. `cryptopulse_product_ready`

Sin etiquetas. 1 si se cumple toda la política integral; 0 si hay motivos de degradación. Alimenta el primer panel de Grafana. Si permanece 0 dos minutos, dispara alerta técnica. No es una puntuación de riesgo de BTC/SOL ni una probabilidad de acierto del modelo.

#### 8. `cryptopulse_recent_missing_minutes{symbol}`

Minutos ausentes entre las últimas 61 velas existentes de cada activo. Se suman saltos internos, no todos los minutos ausentes de todo el histórico. Un valor superior a cero durante 30 segundos dispara alerta. Durante calentamiento puede valer 0 aunque haya menos de 61 filas; por ello debe mirarse también `context_ready`.

#### 9. `cryptopulse_context_ready{symbol}`

1 si el contexto del activo tiene cantidad, continuidad y frescura válidas; 0 si falta alguna. Resume si puede calcularse el conjunto completo de variables retrospectivas. No asegura que el otro activo esté sincronizado ni que se haya emitido ya la serie conjunta.

#### 10. `cryptopulse_prediction_age_seconds{dimension,symbol}`

Edad del ancla de la última predicción live de la versión activa en cada una de las siete series. Si no hay predicción se publica infinito, de modo que no se disfraza como cero. Más de 180 segundos durante un minuto dispara alerta crítica. Mide antigüedad de información del mercado, no tiempo de CPU gastado en calcularla.

#### 11. `cryptopulse_matured_predictions_pending{dimension}`

Cantidad de predicciones live sin `actual` cuyo horizonte ya terminó según reloj. Incluye versiones anteriores, porque su SQL no limita a la versión activa. Sirve para descubrir acumulación de evaluación pendiente. Puede crecer por huecos, ausencia del registro antiguo o capacidad de evaluación. No hay un umbral de alerta automática configurado para esta familia.

#### 12. `cryptopulse_batch_duration_seconds{query}`

Duración `triggerExecution` del último progreso de cada consulta, convertida de milisegundos a segundos. Compararla con el trigger de 15 segundos orienta sobre capacidad: lotes sostenidamente más lentos pueden acumular atraso. No es media histórica, percentil p95 ni suma acumulada; el valor se conserva hasta otro progreso. Incluye los trabajos de la consulta, y para archivo puede reflejar el costo de inferencia y evaluación dentro de su ejecución.

#### 13. `cryptopulse_input_rows_per_second{query}`

Tasa de entrada reportada por Spark en su último progreso. Es distinta de las publicaciones del productor y de velas únicas insertadas. Un lote de recuperación puede elevarla; un tramo sin nuevos cierres puede bajarla a cero. No mide USDT por segundo.

#### 14. `cryptopulse_watermark_dropped_rows{query}`

Suma de `numRowsDroppedByWatermark` en los operadores de estado del último progreso. Sirve para detectar datos que las ventanas consideran demasiado tardíos. No es un contador histórico total ni el número de todos los rechazos de contrato. Puede bajar en el siguiente progreso porque es Gauge de la última captura. La consulta archivo no usa watermark para excluir velas antiguas.

#### 15. `cryptopulse_kafka_checkpoint_lag{topic}`

Suma por topic de `max(0, offset_final_broker − offset_fin_procesado_archivo)` en cada partición. El offset procesado se obtiene del último progreso de `cryptopulse_archive`. Un valor 0 indica que ese progreso ha alcanzado las posiciones disponibles consultadas; no certifica sincronía de las otras dos consultas ni la verdad futura de los modelos. Más de 100 durante dos minutos dispara alerta.

Si faltan offsets de progreso, la referencia usada puede ser cero y el lag aparente ser alto durante arranque. Un fallo de la consulta Kafka se refleja en `kafka_probe_up`, y la familia de lag puede no tener una muestra nueva; una ausencia no equivale a cero.

#### 16. `cryptopulse_kafka_probe_up`

Sin etiquetas. 1 si se pudieron consultar particiones y offsets; 0 ante excepción. Confirma capacidad de consulta del broker en esa captura. No hay regla propia de alerta configurada; es útil para interpretar un lag ausente y complementar el estado del target kafka-exporter.

#### 17. `cryptopulse_last_candle_age_seconds{symbol}`

Edad del mayor `close_ms` archivado por activo, acotada a no negativa. Su SQL básico cuenta todos los modos, incluido replay; el control integral utiliza una consulta más estricta que excluye replay. Con el histórico de prueba anterior al vivo esto no altera el máximo vivo, pero la diferencia de filtros debe conocerse si se incorporan otros replays. Más de 180 segundos durante un minuto genera alerta de mercado desactualizado.

#### 18. `cryptopulse_archived_candles{symbol}`

Número de filas únicas de `market_candles` por símbolo. Incluye los modos presentes. Es el archivo ingerido por la ruta viva/pruebas y no los casi cinco millones de velas batch Parquet. Puede crecer al reparar un minuto antiguo aunque el último precio no cambie. No tiene umbral de alarma: un volumen de archivo mayor no implica mayor riesgo.

#### 19. `cryptopulse_rejected_events`

Sin etiquetas. Cantidad de hashes rechazados persistidos en `rejected_events`. Un crecimiento sugiere revisar motivos y payloads. No incluye automáticamente rechazos del productor anteriores a Kafka ni repeticiones idénticas ya existentes. No está configurada una regla automática por su valor.

#### 20. `cryptopulse_evaluated_predictions{dimension,source_mode}`

Número de predicciones con resultado real incorporado por dimensión y modo. Agrega símbolos y versiones según la consulta actual. Sirve para observar maduración, pero no mide precisión, recall ni rendimiento económico. La etiqueta permite separar replay de live al consultar. El tablero de calidad utiliza SQL con versión y período para evitar mezclar esos universos.

#### 21. `cryptopulse_estimated_monthly_cost_usd`

Sin etiquetas. Lee `_runtime/costs.json`, generado desde hipótesis configurables. Es un escenario ilustrativo en USD, no una factura de Docker ni un cálculo a partir del uso real. Si no existe la configuración de resultado, el código expone cero; ese cero no demuestra que la infraestructura carezca de costos.

### Métricas externas usadas y límites de sus nombres

`up{job=...}` la genera Prometheus tras intentar cada scrape: 1 si responde, 0 si falla. No la emite CryptoPulse como atributo financiero. `kafka_topic_partition_current_offset{topic,partition,...}` procede de Kafka Exporter y se suma por topic en el gráfico de avance. Su tasa podría consultarse aparte, pero el panel actual muestra niveles acumulados.

Kafka Exporter y Prometheus pueden exponer otras métricas internas de sus versiones. No todas se usan en el tablero ni son KPIs propios del proyecto. El catálogo anterior enumera de forma completa las 21 familias implementadas en `exporter.py` y aquí se identifican además las externas efectivamente usadas por sus paneles y reglas.

### Ocho reglas técnicas, con umbral y permanencia

| Regla real | Expresión | Tiempo continuo (`for`) | Severidad y significado |
|---|---|---|---|
| `CryptoPulseExporterUnavailable` | `up{job="cryptopulse"} == 0` | 1 minuto | critical; no puede recogerse la instrumentación del producto. |
| `CryptoPulseStaleMarket` | `cryptopulse_last_candle_age_seconds > 180` | 1 minuto | warning; falta cierre archivado reciente para un activo. |
| `CryptoPulseStreamingStopped` | `cryptopulse_stream_heartbeat_age_seconds > 60` | 1 minuto | critical; el driver no publica vida reciente. |
| `CryptoPulseKafkaLag` | `cryptopulse_kafka_checkpoint_lag > 100` | 2 minutos | warning; atraso de offsets del archivo. |
| `CryptoPulseWebSocketDisconnected` | `cryptopulse_websocket_age_seconds > 90` | 1 minuto | warning; revisar transporte y recuperación REST. |
| `CryptoPulseStalePrediction` | `cryptopulse_prediction_age_seconds > 180` | 1 minuto | critical; una serie activa no actualiza información. |
| `CryptoPulseMarketGap` | `cryptopulse_recent_missing_minutes > 0` | 30 segundos | warning; falta un minuto interno en contexto. |
| `CryptoPulseProductDegraded` | `cryptopulse_product_ready == 0` | 2 minutos | warning; falló algún requisito integral. |

El tiempo `for` empieza cuando la condición se observa como verdadera y debe mantenerse. Se agrega al tiempo de antigüedad que ya exige la expresión; una regla de edad >180 durante un minuto no se activa inmediatamente al pasar un minuto sin una vela. La resolución de evaluación es de 15 segundos y puede añadir demora de muestreo.

En `http://localhost:49090/alerts`, INACTIVE significa que la condición no está activa; PENDING significa que se cumple pero aún no alcanzó su permanencia; FIRING significa que ya la alcanzó. Esos son estados de reglas técnicas, diferentes de NORMAL/ALERTA del centro de señales de mercado.

No hay Alertmanager, canal de correo, Telegram, WhatsApp o notificaciones externas configurado. Las reglas se consultan en Prometheus y sus datos pueden verse en Grafana. El flujo de mercado no crea automáticamente una regla Prometheus por cada probabilidad alta.

### Escenario de costos, explicado completamente

`actualizar_costos.ps1` usa:

```text
costo = horas_mes × (cores_CPU × tarifa_core_hora + RAM_GiB × tarifa_GiB_hora)
        + disco_GiB × tarifa_GiB_mes
      = 730 × (3,5 × 0,02 + 5,5 × 0,003) + 20 × 0,1
      = 65,145 USD/mes, aproximadamente 65,15
```

Las hipótesis son editables en `config/cost-assumptions.json`. CPU 3,5 y RAM 5,5 son tamaños supuestos del escenario, no mediciones de los contenedores. No incluye automáticamente tráfico, electricidad local, amortización del ordenador, copias o impuestos. `_runtime/costs.json` conserva fecha y `kind='illustrative_estimate'` para impedir que se presente como costo facturado.
