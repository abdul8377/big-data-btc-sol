# Del dato al resultado: batch, ingesta y streaming

## Datos históricos, preparación batch y calidad

### Qué representa exactamente una fila

La unidad de observación de CryptoPulse es una **vela cerrada de un minuto del mercado Spot de Binance**. Una vela resume las operaciones que ocurrieron en un intervalo de un minuto. No es una orden del libro, una operación individual, un precio tomado de una página ni una cotización de futuros. `BTCUSDT` representa BTC cotizado en USDT y `SOLUSDT` representa SOL cotizado en USDT. USDT es la moneda de cotización; el proyecto no debe llamar USD a estos importes sin aclarar la diferencia.

Los doce campos del CSV de origen, en orden, son:

1. `open_time_raw`: instante de inicio del intervalo; su unidad cambia según el mes del archivo.
2. `open`: precio de la primera operación de la vela, en USDT por unidad del activo.
3. `high`: precio máximo de la vela.
4. `low`: precio mínimo de la vela.
5. `close`: precio de la última operación de la vela; es la base de los rendimientos utilizados por los modelos.
6. `volume`: volumen negociado expresado en el activo base: BTC o SOL.
7. `close_time_raw`: final del intervalo.
8. `quote_asset_volume`: importe negociado expresado en USDT; permite comparar actividad sin confundir cantidad de monedas con importe.
9. `number_of_trades`: número de operaciones incluidas en la vela. No es número de personas, cuentas ni órdenes únicas.
10. `taker_buy_base_volume`: volumen de compras iniciadas por el tomador de liquidez, en el activo base.
11. `taker_buy_quote_volume`: ese volumen de compra expresado en USDT.
12. `ignore`: campo del formato de Binance que el proceso limpio descarta.

Los campos `taker_buy_*` se conservan en el conjunto limpio, pero no forman parte de las catorce variables de los modelos actuales. Su presencia no significa que CryptoPulse tenga implementado un análisis de desequilibrio comprador/vendedor.

El precio y los volúmenes originales se leen con `DecimalType(28,8)`. Así, Spark usa una representación decimal con hasta 28 dígitos totales y 8 decimales al validar y almacenar esos valores. En los cálculos estadísticos y modelos se convierten expresamente a `double`, porque los logaritmos, covarianzas y estimadores operan con números de punto flotante. Esta conversión es intencional y no convierte la salida estadística en una medición de precisión monetaria exacta.

### Procedencia y trazabilidad

La descarga histórica mensual utiliza esta plantilla real:

```text
https://data.binance.vision/data/spot/monthly/klines/{SYMBOL}/1m/{SYMBOL}-1m-{AAAA-MM}.zip
```

Cada ZIP tiene un archivo acompañante `.CHECKSUM`. `scripts/descargar_historicos_s02.py` descarga el checksum oficial, calcula SHA256 del ZIP recibido y exige coincidencia antes de aceptarlo. Si ya existe una copia local cuyo hash coincide, reutiliza ese ZIP. El CSV se extrae leyendo el único miembro CSV del archivo; no se extraen rutas arbitrarias del ZIP. Se escribe primero un archivo temporal `.part` y después se reemplaza el destino.

El manifiesto `pyspark/sesiones/s02-fundamentos/data/provenance.json` conserva símbolo, mes, URL, URL del checksum, hash del ZIP, hash del CSV, filas, filas teóricas del calendario y unidad temporal de origen. `synthetic: false` indica que el origen descargado es real; la validación posterior determina si esas filas cumplen el contrato. SHA256 detecta cambios o corrupción respecto de la copia cuya huella se verificó; no demuestra que un precio del proveedor sea económicamente correcto.

La descarga usa hasta tres intentos ante errores recuperables de red/servidor y una espera exponencial corta. Un 404 produce un error explícito: no se sustituye el archivo ausente por datos simulados. Solo admite meses UTC cerrados. El script, ejecutado sin argumentos, tiene valores iniciales reducidos `2024-12` a `2025-01`; el intervalo del producto completo está en `config/pipeline.json`, de `2022-01` a `2026-09`, y los scripts del producto le pasan ese rango. No debe confundirse el valor predeterminado del ejercicio inicial con el histórico realmente preparado.

La sesión S01 usó una muestra diaria distinta: 1 de enero de 2025, 1.440 velas por activo y 2.880 filas en total. Se descargó desde `data/spot/daily/klines/...`, se comprobó SHA256 y se generó `cryptopulse_klines_muestra.csv`. Su misión fue comprobar el entorno Spark y las primeras transformaciones. Esa muestra no es el entrenamiento completo ni es un stream en vivo.

### Dimensión del histórico validado

La ejecución registrada de S03 informa:

| Magnitud | Valor registrado | Interpretación |
|---|---:|---|
| Meses | 57 | Enero de 2022 a septiembre de 2026, ambos incluidos. |
| Archivos mensuales | 114 | 57 meses multiplicados por 2 activos. |
| Filas originales | 4.993.759 | Suma de los archivos descargados. |
| Filas inválidas | 2 | Se conservaron en cuarentena. |
| Filas limpias | 4.993.757 | Entradas admitidas tras calidad. |
| BTCUSDT limpio | 2.496.879 | Velas válidas de BTC. |
| SOLUSDT limpio | 2.496.878 | Velas válidas de SOL. |
| Claves con precios/actividad contradictorios | 0 | No se encontraron conflictos en esa ejecución. |
| Filas eliminadas por duplicación/conflicto adicional | 0 | No hay una deduplicación masiva ocultando registros. |
| Tramos con huecos | 3 | Discontinuidades entre velas consecutivas. |
| Minutos-activo faltantes | 163 | La suma por activo; no equivale necesariamente a 163 minutos distintos de calendario. |

El primer inicio de vela es `1640995200000` y el último `1790812740000`, ambos en milisegundos UTC. El informe de calidad persiste en `_runtime/s03.json` y `s03-calidad/artifacts/verificacion_s03.json`. Los contadores no significan que se hayan corregido los huecos inventando precios: el sistema conserva y comunica esos huecos.

### Normalización temporal

Los archivos históricos anteriores a enero de 2025 utilizan milisegundos. Desde enero de 2025, los archivos utilizados tienen marcas en microsegundos. `quality.load_raw()` identifica el mes a partir del nombre del CSV y, cuando corresponde, realiza división entera por 1.000. Los campos normalizados son `open_ms` y `close_ms`.

En una vela normalizada de un minuto se exige:

```text
open_ms % 60000 = 0
close_ms - open_ms = 59999
```

`event_time` se crea a partir de `close_ms`; por tanto, representa el final de la vela. Spark trabaja con `spark.sql.session.timeZone=UTC`. Los campos `year`, `month` y `date_utc` se derivan de ese tiempo normalizado. Esto evita interpretar como una fecha remota un timestamp de microsegundos tratado erróneamente como milisegundos.

### Reglas de calidad y qué ocurre al fallar

`quality.mark_quality()` construye `quality_errors`, una lista que puede contener varios motivos en la misma fila:

- `required_null`: falta al menos uno de los campos fundamentales de símbolo, tiempos, OHLC o actividad. No todos los campos auxiliares del proveedor son obligatorios para este control; `taker_buy_*` e `ignore` no forman parte de esa lista mínima.
- `unknown_symbol`: el símbolo no pertenece a BTCUSDT o SOLUSDT.
- `invalid_ohlc`: existe precio no positivo, `high` es menor que apertura/cierre, `low` supera apertura/cierre o el máximo es menor que el mínimo.
- `negative_activity`: volumen base, volumen en USDT o número de operaciones negativo.
- `invalid_interval`: el minuto no está alineado o su duración normalizada no es exactamente 59.999 ms entre inicio y cierre.

La lectura CSV tiene esquema explícito y modo `FAILFAST`: un error de parseo incompatible con el esquema puede detener la lectura antes de obtener una fila marcada. Las reglas de cuarentena actúan sobre las filas que sí pudieron convertirse al esquema. Conviene distinguir un fallo de carga estructural de una fila cargada pero semánticamente inválida.

Las filas inválidas se escriben en `s03-calidad/artifacts/quarantine`. Para las filas válidas, el código agrupa por `(symbol, open_ms)` y compara las versiones de `(open, high, low, close, volume, quote_asset_volume, number_of_trades)`. Si una misma clave trae valores contradictorios en ese conjunto, conserva la evidencia en `conflicting_duplicates` y excluye la clave de los datos limpios. No selecciona de forma arbitraria uno de los precios. Después aplica `dropDuplicates` sobre la clave para eliminar repeticiones equivalentes. Los campos auxiliares `taker_buy_*` no forman parte de la comparación de conflicto: es una limitación concreta del control actual.

Finalmente calcula el salto entre `open_ms` y el de la fila anterior del mismo activo. Un salto superior a 60.000 ms genera un registro en `s03-calidad/artifacts/gaps`. El número faltante es `delta_ms/60000 - 1`. No hay interpolación, relleno hacia delante ni sustitución por cero en los precios.

### Por qué Parquet y por qué esas particiones

El conjunto limpio se escribe como Parquet bajo `s03-calidad/artifacts/clean_parquet`, particionado por `symbol/year/month`. Parquet conserva tipos, almacena por columnas y reduce la lectura necesaria cuando solo se consultan ciertas variables. El particionado permite que una consulta de BTC de enero de 2025 descarte carpetas de SOL y de otros meses antes de leer todos sus contenidos.

La escritura usa `repartition(16, symbol, year, month)` y Spark configura 16 particiones de shuffle para estas tareas locales. Esto reparte trabajo dentro del proceso Spark disponible; no significa que existan dieciséis servidores. S03 verifica que el plan de una consulta filtrada contiene `PartitionFilters` y guarda ese plan en `partition_pruning.txt`.

Hay un archivo original CSV, un ZIP y varias representaciones derivadas porque cumplen funciones distintas: reproducibilidad de la fuente, copia compacta, conjunto tipado de calidad y datos listos para aprendizaje. Esa coexistencia explica una parte importante del tamaño de la carpeta.

## Ingesta de datos nuevos: captura, contrato y publicación

### Fuente en vivo y diferencia entre un mensaje y una vela

El productor se conecta a:

```text
wss://data-stream.binance.vision/stream?streams=btcusdt@kline_1m/solusdt@kline_1m
```

La conexión combinada entrega actualizaciones de ambos activos. `from_websocket()` abre la envoltura `data`, verifica `e='kline'` y exige que `k.x` confirme el cierre. Las actualizaciones parciales sirven para saber que la conexión recibe mensajes, pero no se publican como velas analíticas. Por tanto, el contador de mensajes WebSocket puede crecer mucho más deprisa que el de velas publicadas.

El mercado produce normalmente una vela cerrada por activo y minuto: aproximadamente dos eventos nuevos por minuto en total. La plataforma admite más eventos por recuperación/reintentos; su utilidad Big Data en este despliegue procede principalmente del histórico y la integración de procesamiento, no de un caudal vivo masivo de miles de operaciones individuales por segundo.

La fuente REST de apoyo es `https://data-api.binance.vision`. `/api/v3/time` proporciona la hora del servidor y `/api/v3/klines` devuelve velas con símbolo, intervalo `1m`, inicio, final y límite 1.000. El último inicio cerrado se calcula como `(serverTime // 60000 − 1) × 60000`. Se consulta el minuto anterior completo, no el que todavía está formando su precio final.

Ambas interfaces son públicas de datos de mercado. No se emplea una cuenta Binance ni se descargan saldos, órdenes privadas, identidades de operadores, libro completo, futuros o datos on-chain.

### Contrato de eventos versión 1

`make_event()` convierte REST y WebSocket al mismo objeto. Su identidad estable es `SYMBOL:1m:open_ms`, por ejemplo `BTCUSDT:1m:1793232420000`. La identidad depende del minuto del mercado, no de la hora a la que se recibió o republicó. Una recuperación REST del mismo minuto conserva su identidad.

| Campo | Tipo/valor del contrato | Función |
|---|---|---|
| `schema_version` | Entero 1 | Identifica la versión aceptada del esquema. |
| `event_type` | `market.candle.closed` | Indica que la unidad es una vela finalizada. |
| `event_id` | Cadena de símbolo, intervalo e inicio | Permite deduplicar el mismo hecho entre transportes. |
| `symbol` | BTCUSDT o SOLUSDT | Identifica el mercado. |
| `interval` | `1m` | Declara la duración esperada. |
| `open_ms`, `close_ms` | Enteros UTC en milisegundos | Determinan posición temporal, duración y orden. |
| `event_time` | Fecha ISO UTC generada desde `close_ms` | Representación legible del cierre. |
| `open`, `high`, `low`, `close` | Cadenas decimales | Conservan precios sin pasar primero por un flotante. |
| `volume` | Cadena decimal, BTC o SOL | Cantidad negociada del activo base. |
| `quote_asset_volume` | Cadena decimal, USDT | Importe negociado. |
| `number_of_trades` | Entero no negativo | Cuenta operaciones agregadas. |
| `is_closed` | `true` | Excluye actualizaciones abiertas. |
| `source` | `binance_spot` | Identifica procedencia declarada. |
| `source_mode` | `live`, `backfill` o `replay` | Distingue actualidad, recuperación y prueba histórica. |
| `transport` | `websocket`, `rest` o `replay` | Explica cómo entró el registro. |
| `published_ms` | Entero UTC | Marca el momento de publicación preparado por el productor. |
| `synthetic` | `false` | Declara que no se generó una vela artificial. |

`source_mode` y `transport` son ejes distintos. Una vela REST de los últimos 120 segundos se publica como `live`; una más antigua, como `backfill`. Por ello, «live» no equivale necesariamente a «recibida por WebSocket». `replay` identifica datos reales históricos utilizados en pruebas, no una simulación del precio.

El JSON Schema está en `schemas/market-kline-v1.schema.json`, con una copia junto a los módulos. El recorrido de ejecución no invoca un motor JSON Schema: usa `contract.validate()`. La validación Python agrega reglas de negocio, pero no es idéntica al archivo declarativo. Por ejemplo, exige tipos enteros exactos y valores decimales representables, pero no comprueba expresamente el formato de `event_time` ni rechaza todas las propiedades adicionales. El pipeline deriva su tiempo efectivo de `close_ms`, no de una fecha de texto enviada por un tercero. No hay Schema Registry externo ni negociación automática de versiones.

### Reglas efectivas de validación

Se exige un objeto, versión 1, tipo de evento correcto, símbolo permitido, intervalo 1m, `is_closed=true`, origen declarado y bandera no sintética. Los tipos enteros se comprueban con `type(...) is int`, de modo que un booleano de Python no pasa como número de operaciones o timestamp.

Los tiempos deben ser no negativos, caber en bigint, estar ordenados, empezar en un múltiplo exacto de 60.000 y separarse 59.999 ms. Se toleran como máximo 10 segundos de cierre por delante del reloj local; un desfase mayor se rechaza. `published_ms` debe existir como entero representable; no se calcula a partir de él un SLA completo de latencia.

Cada precio/volumen se convierte a `Decimal`, se rechazan NaN e infinito y se exige representabilidad en `numeric(28,8)`: magnitud menor que `1e20` y hasta ocho decimales efectivos. Los precios deben cumplir `0 < low ≤ min(open,close) ≤ max(open,close) ≤ high`. Volúmenes y operaciones no pueden ser negativos. El identificador debe coincidir exactamente con símbolo e inicio.

Estas reglas protegen la coherencia técnica y semántica del registro. Que el evento declare `source='binance_spot'` no constituye una firma criptográfica del exchange; el productor es quien establece la conexión al origen público. El sistema no consulta un segundo exchange para demostrar que cada precio es correcto.

### Reconciliación REST y protección contra minutos intermedios perdidos

El productor mantiene dos conceptos separados: `last_open_ms`, máximo minuto recibido, y `rest_reconciled_open_ms`, hasta dónde se consultó y confirmó una recuperación. WebSocket actualiza el máximo recibido, pero no mueve el cursor REST. Esta separación corrige el caso en que llega el minuto 12:02 y falta 12:01: el máximo no demuestra que los minutos anteriores estén completos.

Al arrancar, primero reconcilia REST y después abre WebSocket. Sin estado previo toma aproximadamente las últimas tres horas de velas cerradas. Si encuentra un estado antiguo sin el nuevo cursor, retrocede unas 24 horas respecto del máximo para revisar posibles pérdidas ocultas. Con cursor establecido revisita al menos las últimas 180 velas y recorre hacia delante lo que haga falta.

La petición se divide en páginas de hasta 1.000 minutos. Cada respuesta debe ser una lista de velas de estructura válida; se ordena por inicio y se exige que cada vela pertenezca al tramo cerrado pedido. Tras publicar todas las velas de una página y recibir sus confirmaciones, avanza el cursor de esa página. Un fallo de ACK no confirma la página como terminada.

Durante la conexión, la reconciliación se repite aproximadamente cada 60 segundos. El productor puede así recuperar una vela perdida aunque otra posterior ya se hubiera recibido. Calcula también qué minutos del tramo no devolvió la fuente y los registra en `source_gaps`, con cantidad y una lista limitada de inicios. Ese diagnóstico no rellena precios ni impide por sí solo todo avance del cursor; la revisión solapada vuelve a intentar el tramo reciente. Un hueco muy antiguo puede requerir reparación explícita.

`ReconciledPublisher` conserva en memoria un `OrderedDict` de hasta 12.000 identidades vistas para evitar republicaciones repetitivas durante el mismo proceso. Ese conjunto no es un inventario duradero de todos los eventos históricos; al reiniciar puede volver a publicar registros que PostgreSQL ya tenga. Esa repetición está contemplada por las claves de persistencia.

### Confirmaciones, reintentos y reconexión

El productor Kafka utiliza `enable_idempotence=True`, `acks='all'` y diez reintentos. Para cada envío espera `.get(timeout=30)` antes de contar el evento como publicado. Serializa la clave como símbolo y el valor como JSON. El estado persistido incluye publicaciones acumuladas, publicaciones live/backfill, últimos inicios, cursor REST, última hora de cierre y último transporte.

La conexión WebSocket tiene timeout de recepción de cinco segundos. Recibir un timeout no fabrica una vela: permite volver al bucle y efectuar reconciliación/heartbeat. Ante una excepción general se marca desconectado, se conserva el error, se cierra la conexión y se reintenta con espera exponencial de 1 a 30 segundos. El contador de reconexiones y de mensajes corresponde a la ejecución actual; ciertos acumulados de publicación se recuperan del JSON anterior.

El heartbeat del productor se intenta actualizar cada diez segundos en el bucle normal y al confirmar eventos o páginas. Una operación de red lenta puede retrasarlo. La salud considera atrasos superiores a 60 segundos; no asume que `connected=true` demuestre continuidad.

El guardado de JSON compartido crea un temporal único en el mismo directorio, escribe/flush/fsync y reemplaza el destino. Si Windows bloquea temporalmente el reemplazo, reintenta hasta siete intentos con espera creciente. El lector puede encontrar un estado anterior completo mientras se publica el nuevo, en lugar de un JSON escrito a medias. Un fallo persistente sigue siendo un error observable, no se oculta indefinidamente.

### Reparación dirigida de huecos

`producer --repair-gaps --hours 72` consulta discontinuidades internas entre registros de PostgreSQL dentro del período, pide las velas reales a Binance y las republica como `backfill/rest`. Admite de 1 a 744 horas. Guarda `ingestion_repair.json` con rangos, cantidad republicada y minutos no disponibles.

La herramienta identifica huecos entre dos registros presentes; no certifica que exista todo dato anterior al primero, posterior al último o fuera del rango. Su PASS indica que publicó los minutos encontrados y disponibles. La verificación de producto comprueba después que Spark los archivó y el contexto reciente quedó utilizable. En la corrección anterior se recuperaron dos velas reales con inicio `1793232420000`, una por activo.

## Kafka y procesamiento continuo con estado

### Topics, particiones y significado de los offsets

El productor crea, si no existen, `market.klines.btcusdt`, `market.klines.solusdt`, `market.deadletter` y `cryptopulse.integration`, con tres particiones y replicación 1. Las pruebas pueden crear además topics aislados con sufijo único.

Los dos primeros transportan el mercado. `cryptopulse.integration` se utiliza para la prueba de ida y vuelta. **`market.deadletter` está creado, pero el flujo actual guarda los rechazos en la tabla `rejected_events`; no los publica automáticamente en ese topic.** Un nombre configurado no debe confundirse con un circuito de errores implementado.

La clave es el símbolo. Todos los mensajes de un mismo activo se dirigen de forma estable a una partición bajo esa configuración; tres particiones no implican que un único activo se procese en tres flujos paralelos. Kafka ordena por partición, no impone orden de tiempo de mercado entre topics. Una reconciliación puede publicar un minuto antiguo después de uno reciente, y el procesamiento debe tolerarlo.

Un offset es una posición dentro de una partición. La suma de offsets finales indica avance acumulado aproximado, no número de eventos únicos, volumen negociado ni cantidad retenida. Los reintentos y recuperaciones también ocupan posiciones. La retención de 168 horas limita cuánto tiempo puede volver a leerse el log; PostgreSQL y los archivos tienen persistencias diferentes.

### Tres consultas Spark independientes

`stream.run()` define la entrada Kafka y lanza tres consultas; Spark mantiene offsets y checkpoints independientes para cada una. Todas suscriben ambos topics, con `startingOffsets='earliest'`, `maxOffsetsPerTrigger=2000` y `failOnDataLoss=true`.

`startingOffsets` se aplica cuando no existe un checkpoint válido: con checkpoint, la recuperación continúa desde los offsets guardados. `earliest` significa lo más antiguo todavía retenido, no todos los datos desde 2022. `failOnDataLoss=true` busca que una posición perdida por borrado/retención se convierta en error, en lugar de saltarse silenciosamente un tramo.

1. **`cryptopulse_archive`.** Procesa los JSON originales, aplica el contrato, archiva velas/rechazos y ejecuta inferencia/evaluación. No tiene watermark para descartar recuperaciones antiguas del archivo.
2. **`cryptopulse_fixed`.** Valida, deduplica dentro del watermark y agrega ventanas fijas de cinco minutos.
3. **`cryptopulse_sliding`.** Valida, deduplica dentro del watermark y agrega ventanas de cinco minutos con paso de un minuto.

Cada una utiliza trigger de procesamiento de 15 segundos. Es un intervalo de planificación: si un lote tarda más, no se garantiza que el siguiente termine a los 15 segundos exactos. Se registran duración y tasa para observar esa diferencia.

### Qué hace el micro-lote del archivo

Si el lote está vacío, retorna. Si contiene registros, recoge hasta el límite de entrada en el driver, interpreta JSON y valida cada evento. Los aceptados se envían en lote a `market_candles`. Los inválidos se guardan con SHA256 del texto, motivo y payload en `rejected_events`; una clave repetida no crea una fila nueva. Un tombstone Kafka, cuyo valor es nulo, también se trata como rechazo sin derribar por ello el archivo.

Si hay modelo habilitado y eventos válidos, obtiene las velas recientes de PostgreSQL, llama a `ModelBundle.predict()`, inserta las predicciones y evalúa resultados maduros. Finalmente registra `stream_batches`, con número de registros recibidos, duración, nuevas velas únicas y rechazados. No crea una fila de lote cuando estaba vacío.

La implementación usa `collect()` y escrituras desde el driver, adecuada para el caudal reducido descrito. No distribuye la escritura de cada partición Spark a varios escritores PostgreSQL; ese punto requeriría otro diseño si aumentan mucho los activos o eventos.

### Ventanas fijas y deslizantes: qué calculan

Las ventanas usan **tiempo de evento**, derivado de `close_ms`, y no la hora a la que Spark recibió el JSON. Cada una calcula `candles`, suma de `quote_asset_volume` y media aritmética de los cierres. `avg_close_usdt` no es VWAP: no pondera por volumen. El OHLC horario y VWAP del notebook S02 pertenecen a otro análisis.

Una ventana fija de cinco minutos agrupa intervalos consecutivos sin solapamiento. Una deslizante dura cinco minutos y comienza cada minuto: una vela puede contribuir a cinco ventanas distintas. No debe sumarse su volumen entre todas ellas para obtener un total sin descontar solapamientos.

Se usa `withWatermark('event_timestamp','2 minutes')` y `dropDuplicatesWithinWatermark(['event_id'])`. El watermark sigue el progreso del mayor tiempo de evento observado, con margen de dos minutos; no es un reloj de pared que cierre todo exactamente dos minutos después. Si dejan de llegar eventos, puede no avanzar el cierre de ventanas.

La salida es `append`: se escriben resultados una vez que Spark considera cerrada la ventana según su estado y watermark. Un evento demasiado tardío puede descartarse en estas consultas aunque el archivo lo acepte. `final=true` significa «resultado finalizado por el motor», no «contiene cinco velas garantizadas». La tabla de cobertura cuenta ventanas con `candles<>5` para hacer visible esa diferencia.

Una recuperación antigua no reabre automáticamente una ventana cuyo estado ya fue purgado. Recupera la vela en el archivo y puede permitir recalcular variables recientes, pero la agregación histórica de ventanas necesitaría una reconstrucción explícita si se quiere corregir también aquella salida. No hay un proceso automático de reparación retroactiva de todas las ventanas finalizadas.

### Checkpoints, reinicios y garantía real

Los checkpoints están en `_runtime/checkpoints/archive`, `fixed` y `sliding`. Incluyen metadatos de la consulta, offsets/commits y, para las consultas con estado, sus datos de estado. Permiten continuar tras reinicio sin comenzar por todos los eventos retenidos.

La garantía del producto es **entrega al menos una vez con efectos idempotentes por clave de negocio**, no una transacción global exactamente una vez entre Kafka y todas las tablas SQL. Cada bloque de escritura confirma su propia transacción. Si el proceso cae después de archivar velas y antes de terminar inferencia, el lote puede repetirse; las claves evitan repetir velas/predicciones existentes y permiten completar otras escrituras.

`market_candles` conserva la primera fila por símbolo/minuto mediante `ON CONFLICT DO NOTHING`; no reemplaza silenciosamente un precio anterior por otra versión de la fuente. Eso protege inmutabilidad de identidad, pero no incorpora una auditoría de correcciones del proveedor ni compara en vivo cada duplicado contradictorio. Los KPI y señales permiten actualizar valores de una misma identidad cuando se recupera contexto; las predicciones conservan su primera estimación.

No deben borrarse checkpoints para «solucionar» un atraso sin entender sus consecuencias. Cambiar esquemas de estado, particionado o identidad de consulta puede requerir un procedimiento de migración. Si Kafka ya eliminó offsets necesarios, hay que diagnosticar recuperación y alcance; el reinicio por sí solo no reconstruye mágicamente un log perdido.

El bucle del driver verifica cada cinco segundos que las tres consultas estén activas y sin excepciones, publica `stream_status.json` y atiende SIGTERM para detenerlas. Compose concede 60 segundos de gracia al streaming. Las excepciones de consulta se propagan para que un proceso fallado no siga anunciándose como sano.

## Inferencia, evaluación futura y señales explicables

### Carga del modelo y cálculo de las siete series

Al iniciar, `ModelBundle` lee el registro activo, asegura el esquema de producto, registra su versión en PostgreSQL y carga los `PipelineModel` seleccionados que no sean baseline. Mantiene esos modelos en memoria. No vuelve a entrenarlos por evento ni consulta un servicio externo de predicción.

`db.recent_candles(limit=180)` trae hasta 180 velas por símbolo, excluyendo `replay`. Las variables se construyen con `past_features`, igual que en batch. El cálculo por activo selecciona únicamente su última vela y descarta filas con alguna de las catorce entradas nula. Puede emitir algunos KPI observados de horizonte corto aunque todavía falte contexto para todas las predicciones.

Los tres modelos compartidos por activo producen dos salidas cada uno. El par solo se calcula si el último minuto de BTC y SOL coincide exactamente y sus treinta variables son válidas. Una correlación indefinida o un hueco puede dejar sin salida el par; no se reutiliza una pareja antigua como si fuera actual.

Si el método fuera baseline, `score()` genera su referencia: `rv15` para volatilidad, prevalencia histórica por activo para clasificación o cero para divergencia. Con el registro actual se aplican Random Forest, dos logísticas calibradas y la referencia cero de divergencia. El resultado de volatilidad se limita a no negativo.

El modo de la salida por activo procede del ancla. Para el par es live si ambos anclajes son live, replay si ambos son replay y backfill en otro caso. El tablero principal filtra predicciones live y versión activa. La persistencia y las pruebas sí pueden conservar otros modos sin presentarlos como producción actual.

`inference_status.json` registra versión, cantidad de series, sincronización, diagnóstico de continuidad y un motivo. Las ejecuciones que solo contienen replay no sobrescriben ese diagnóstico vivo. `market_kpis.json` conserva además el último KPI de correlación/divergencia calculado; la fuente del tablero es PostgreSQL.

### Momento de emisión frente a momento del mercado

`as_of_ms` es el cierre de la vela ancla; `issued_at` es cuándo PostgreSQL guardó la predicción. `horizon_minutes` define cuándo se puede conocer su resultado. `observed` describe una condición ya medida al emitirla, mientras `actual` representa la verdad posterior del objetivo.

En volatilidad, `observed` es `rv15` reciente; en caída, el rendimiento retrospectivo `r30`; en actividad, un flag de anomalía ya observada; en divergencia, el spread retrospectivo. No todos los `observed` tienen la misma unidad que el pronóstico: la probabilidad de caída no debe compararse numéricamente como si fuera el rendimiento reciente. Los gráficos y tablas los separan según dimensión.

### Evaluación madura y estados de pendientes

La evaluación se dispara con micro-lotes válidos. Consulta hasta 500 predicciones con `actual IS NULL` cuyo horizonte cabe bajo el mayor cierre archivado, ordena por última comprobación nula/antigua y ancla y espera 30 segundos para reintentar una ya examinada.

El máximo cierre global solo sirve como filtro de candidatos. `actual_for()` exige la secuencia exacta de minutos para el activo o para ambos activos del par. Aunque otro activo esté más adelantado, una serie incompleta no obtiene un resultado inventado.

Cada predicción usa su registro en `model_versions`, incluyendo umbrales de esa versión. Si falta, se marca `unknown_model`; si faltan velas consecutivas, `missing_data`; al completarse, `evaluated`, con `actual`, `evaluated_at` y `evaluation_checked_at`. `evaluation_reason` explica lo que impide completar. Una predicción reciente pendiente porque su horizonte aún no pasó es normal; una madura que no se evalúa requiere revisar el motivo y el backlog.

La estrategia rotativa evita que las primeras predicciones antiguas con huecos ocupen siempre el límite de 500. La evaluación no cambia `forecast` ni usa el resultado real para corregir retrospectivamente lo que se dijo antes. No se realiza aprendizaje online después de cada evaluación.

### Las catorce evaluaciones y sus reglas exactas

`signals.build_signals()` produce observaciones y pronósticos con valor, umbral, unidad, método, versión, modo, explicación y acción de revisión. No guarda solo alarmas: una evaluación normal también es un hecho útil.

| Dimensión | Regla observada | Regla predictiva | Unidad |
|---|---|---|---|
| Volatilidad, BTC y SOL | `rv15 >= P95_volatilidad_activo` | `forecast >= mismo P95` | Escala porcentual de volatilidad |
| Caída, BTC y SOL | `r30 <= P5_retorno_activo` | `p_calibrada >= corte_probabilidad` | Observación en %, probabilidad en 0–1 |
| Actividad, BTC y SOL | `max(quote5/P95_quote, trades5/P95_trades) >= 1` | `p_calibrada >= corte_probabilidad` | Ratio observado, probabilidad en 0–1 |
| Divergencia, par | `abs(r15_SOL-r15_BTC) >= P95_abs_spread` | Mismo criterio de magnitud si se desplegara un modelo útil | Puntos porcentuales |

La referencia cero de divergencia se conserva en las predicciones, pero se marca como referencia y no activa una alerta predictiva sustentada en ML. Si faltan valores finitos o umbral, la evaluación se considera no disponible; un nulo no se transforma en una señal normal.

Un cruce de caída recibe severidad `high`; otros cruces reciben `warning`. Sin cruce y con datos válidos, `normal`; sin disponibilidad, `unavailable`. En la vista del tablero eso se convierte en ALERTA, VIGILANCIA, NORMAL, NO DISPONIBLE o REFERENCIA, con prioridad de estados de ausencia/antigüedad.

`expires_ms` guarda ancla más tres minutos para observaciones y ancla más horizonte para pronósticos. Es metadato persistido; **la vista actual usa un corte de frescura de 180 segundos para todas las filas y no utiliza `expires_ms` para decidir su estado**. El horizonte de validez del objetivo y la exigencia de una estimación reciente son conceptos diferentes. La aplicación no borra una señal al vencer ese tiempo.

`cp_signal_board` construye siete combinaciones por dos tipos y busca su última señal live de la versión activa. Cuando la configuración de versión existe, puede devolver las catorce filas incluso si faltan evaluaciones, con SIN EVALUAR. Si no existe `active_model_version`, su join de configuración no produce filas; la API lo detecta como degradación al no contar catorce.

`/api/signals` clasifica el conjunto como `degraded` si hay ausencia, indisponibilidad o antigüedad; si todas están disponibles y hay cruces, `alerts`; si están disponibles y no hay cruces, `no_alerts`. El historial filtra `triggered=true`; un período sin eventos debe mostrar una explicación y conservar el diagnóstico actual, no llenar la tabla con avisos artificiales.

## Diccionario de persistencia y trazabilidad

Todas las tablas pertenecen a la base `cryptopulse`, esquema público. Los timestamps SQL de auditoría son `timestamptz`; los tiempos de mercado `*_ms` son bigint UTC. Las columnas JSONB guardan estructuras, no un archivo externo que Grafana deba abrir.

### `market_candles`: archivo de velas ingeridas

Clave primaria: `(symbol, open_ms)`. `close_ms` cierra el intervalo; `open/high/low/close`, `volume` y `quote_asset_volume` son `numeric(28,8)`; `number_of_trades` es bigint. `source_mode`, `transport` y `published_ms` conservan procedencia. `received_at` es la recepción en PostgreSQL.

Se inserta una sola fila por minuto y activo. No incluye los casi cinco millones de registros raw del batch: esa ruta conserva su detalle en Parquet y publica resúmenes. La tabla sí crece con ingesta y recuperación viva; no hay una política automática de eliminación por antigüedad implementada para ella.

### `predictions`: estimación original y verdad futura

Clave primaria: `(dimension, symbol, as_of_ms, model_version)`. `dimension` identifica objetivo; `symbol` es activo o `BTC-SOL`; `horizon_minutes` define el horizonte. `forecast` es la predicción, `baseline` su referencia, `observed` el contexto retrospectivo y `actual` el resultado posterior cuando exista.

`method`, `source_mode`, `alert` y `model_version` describen cómo y con qué versión se calculó. `issued_at` audita emisión; `evaluated_at` audita completado; `evaluation_status`, `evaluation_reason` y `evaluation_checked_at` permiten investigar pendientes. El flag `alert` de esta tabla corresponde al pronóstico; las señales observadas y sus textos están en `market_signals`.

No hay clave por `source_mode`: un mismo objetivo/ancla/versión no admite dos predicciones distintas por etiquetarlas con otro modo. Las pruebas usan períodos controlados y filtros para evitar confundir resultados de versiones. Las tablas no deben tratarse como un sandbox sin consecuencias por contener el campo replay.

### `market_kpis`: indicadores retrospectivos

Clave `(symbol, as_of_ms)`. `metrics` contiene JSONB con `rv15`, `rv60`, `r1`, `r15`, `r30`, `quote5`, `trades5`, P95 de actividad, `volume_zscore` y procedencia en filas por activo; en el par contiene correlación y spread. Se actualiza ante conflicto de clave para completar/corregir un KPI tras recuperación de contexto. No tiene columna de versión de modelo: sus referencias internas corresponden al cálculo que escribió esa fila.

### `market_signals`: evaluación explicada

Clave `(dimension, symbol, as_of_ms, kind, model_version)`, con `kind` restringido a observed/forecast. `value`, `threshold`, `unit` cuantifican la regla. `triggered`, `severity`, `title`, `explanation` y `action` la hacen interpretable. `method`, `source_mode`, `expires_ms` y `created_at` conservan método, origen y tiempos.

El upsert actualiza valor, umbral, cruce, severidad, título, explicación, acción y expiración para esa clave. El historial no crea un objeto de incidente con apertura/cierre o confirmación por una persona: guarda evaluaciones por ancla. Un episodio sostenido puede producir varias filas consecutivas de cruce.

### `stream_windows`: agregaciones finalizadas

Clave `(kind, symbol, window_start)`, donde kind es fixed o sliding. Guarda `window_end`, número de velas, volumen USDT sumado, media de cierres y flag `final`. Un upsert actualiza la agregación si se vuelve a escribir la misma identidad. No contiene una lista de todos los eventos integrantes ni una garantía de cobertura completa por `final=true`.

### `rejected_events`: rechazos del archivo

`event_hash` es clave primaria; `reason` explica el fallo, `payload` guarda el texto recibido y `received_at` su recepción. El hash evita repetir el mismo rechazo en cada replay del lote. Los rechazos detectados antes de publicar, dentro del productor, se reflejan en su estado/log y no entran por sí mismos en esta tabla, porque nunca llegaron al archivo Kafka.

### `batch_kpis`: resumen histórico por día

Clave `(symbol, date_utc)`. `candles` cuenta velas limpias, `quote_volume_usdt` suma volumen monetario y `trades` suma operaciones. Al sumar candles se obtiene el total limpio histórico mostrado en Grafana. Un día con datos faltantes conserva su conteo real; no se fuerza a 1.440.

### `model_metrics`, `model_versions` y `runtime_configuration`

`model_metrics` tiene clave `(dimension, split, method, model_version)` y JSONB `metrics`. Los splits publicados distinguen candidato test, baseline test y desplegado test. `model_versions` conserva el registro completo por versión y `registered_at`, necesario para evaluar predicciones antiguas con su propia definición de evento. `runtime_configuration` es clave/valor; `active_model_version` determina qué versión seleccionan las vistas de producto.

El registro en disco, la versión usada por el stream y la versión SQL deben mantenerse alineados. La salud compara parte de esa coherencia y el arranque publica metadatos/migraciones. No se obtiene la versión activa tomando el máximo alfabético de todos los modelos guardados.

### `stream_batches`: auditoría de lotes

Clave `(query_name, batch_id)`. Guarda `rows_count`, `duration_ms`, JSONB payload y `updated_at`. La implementación actual registra aquí lotes de archivo (`query_name='archive'`); el progreso de las tres consultas se observa en `stream_status.json` y las métricas de Spark, no se infiere que cada consulta escriba esta tabla por existir query_name.

### Índices, vistas y migraciones

Los índices principales aceleran búsqueda por tiempo, velas recientes por símbolo, predicciones live de versión por dimensión y pendientes de evaluación. No cambian la regla analítica; reducen costo de consultas. `cp_asset_health` calcula continuidad y edades por activo; `cp_signal_board` expone las catorce evaluaciones con estado legible.

`database/init.sql` crea tablas iniciales solo cuando PostgreSQL inicializa un directorio de datos vacío. Las migraciones 002 y 003 añaden señales, registros, campos de evaluación, índices y vistas sobre volúmenes existentes. El arranque las aplica en orden con `ON_ERROR_STOP=1`; usan construcciones idempotentes y conservan datos. `db.ensure_product_schema()` también asegura las extensiones de esquema necesarias para la inferencia. Modificar únicamente init.sql no actualizaría una base ya creada.
