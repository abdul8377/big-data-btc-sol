# CryptoPulse: producto local de vigilancia de riesgos

Proyecto activo: `D:\bigdata`. Esta entrega corrige los fallos de ingesta, inferencia, alertas y verificación detectados en la revisión del 28 de octubre de 2026. Los informes anteriores documentan el estado de sus respectivas fechas; esta guía describe el comportamiento corregido.

## Resultado de la entrega

Versión activa: **`20261029T011759858407Z.cal`**. Su identificador usa UTC; la entrega se realizó el 28 de octubre en Lima. Reutiliza los modelos base de `20261026T223241Z` y corrige la calibración temporal de los clasificadores.

- **40 pruebas de regresión aprobadas:** ingesta, cursor REST/WebSocket, estado atómico, contrato, señales, salud y separación temporal de calibración.
- **5 pruebas con Spark aprobadas:** modelos guardados y velas reales; 7 series y 14 evaluaciones; hueco y recuperación; par atrasado; continuidad de actividad; variables contrastadas con cálculo independiente.
- **Integración actualizada PASS** para esta versión: Kafka, duplicados, watermark, reinicio de checkpoint, siete predicciones de replay evaluadas y prueba de que modificar el futuro no cambia variables del pasado.
- **Interrupción real de 75 segundos PASS:** degradación detectada y recuperación del productor sin huecos.
- **Arranque normal PASS**, con aplicación de migraciones y espera de salud integral.
- **S12 PASS**, con las consultas SQL de los 16 paneles PostgreSQL y siete expresiones de paneles Prometheus comprobadas. El centro de señales devolvió 14 filas y el historial una explicación cuando no hubo cruces.
- **Estabilidad final:** las siete series avanzaron tres cierres de minuto entre las muestras de aceptación; todas estaban recientes, sin huecos de contexto ni duplicados de velas.

En la captura final el estado era `no_alerts`: 13 evaluaciones normales y una referencia de divergencia. Esto describe ese momento, no garantiza el siguiente estado del mercado. La inspección visual automatizada de la pestaña compartida no estuvo disponible porque no coincidía con la sesión accesible; las consultas y los datos sí se verificaron mediante Grafana y sus API.

## Para qué sirve

CryptoPulse ayuda a decidir **qué condiciones del mercado requieren revisión**, por qué y con qué datos. Compara BTC y SOL en cuatro dimensiones diferentes: intensidad de variaciones, riesgo de caída, actividad inusual y separación entre rendimientos.

La utilidad no depende de que haya una alerta encendida siempre. Un analista puede comprobar que los datos están completos, ver si una condición ya ocurre o se está pronosticando, conocer el umbral histórico y contrastarla con las otras dimensiones. También puede comprobar cómo se equivocan los modelos frente a una referencia sencilla.

Es un producto local de monitoreo, no un sistema de ejecución de órdenes. No genera recomendaciones personales de inversión, atribuciones de fraude ni promesas de rentabilidad. La ausencia de cruces no garantiza ausencia de riesgo.

## Uso del tablero

Abre http://localhost:43000/d/cryptopulse-main con usuario `admin` y contraseña `admin2026!`. Recarga la página completa si conservabas abierta la versión anterior; la provisión automática actualiza la definición del dashboard.

1. **Estado integral:** debe indicar `LISTO` y el contador debe llegar a **7/7**. Se comprueban el productor, el driver, la continuidad de 61 velas por activo, la versión del modelo y la edad/retraso de las siete series.
2. **Frescura y continuidad:** revisa BTC y SOL por separado. Los datos de un activo no pueden ocultar el atraso del otro.
3. **Centro de señales:** compara observaciones con pronósticos y lee valor, unidad, umbral, antigüedad, interpretación y qué conviene revisar.
4. **Historial:** muestra cruces reales durante el período elegido. Si no hay cruces, presenta un mensaje explicativo y remite al estado actual.
5. **Gráficos de las cuatro dimensiones:** contrastan evolución reciente, pronóstico y realidad posterior cuando está disponible. No se dibujan resultados futuros antes de que existan.
6. **Evidencia:** compara el método desplegado con su referencia y revisa precisión, recall, errores y evaluación en vivo por activo.
7. **Operación:** comprueba conexión, retraso de Kafka, duración de lotes, cobertura de ventanas y el escenario de costos.

El archivo contiene 28 elementos: cuatro encabezados de sección, una guía y 23 paneles de datos. La zona horaria predeterminada es la del navegador; una URL antigua con `timezone=utc` conserva UTC hasta que cambies esa preferencia en Grafana.

### Estados del centro de señales

| Estado | Significado | Qué hacer |
|---|---|---|
| NORMAL | Evaluación reciente sin cruce de su umbral. | Seguir la evolución y contrastar dimensiones. |
| VIGILANCIA | Cruce real de referencia observada o predictiva. | Revisar valor, umbral, origen y contexto. |
| ALERTA | Cruce de una condición de caída significativa. | Priorizar la revisión; no es una orden de compra o venta. |
| REFERENCIA | Pronóstico constante de divergencia cero seleccionado por validación. | Usar la divergencia observada para detectar desacoplamiento; no atribuir ventaja predictiva al cero. |
| SIN EVALUAR / NO DISPONIBLE | Falta una evaluación o una variable válida. | Revisar continuidad, sincronización o calentamiento. |
| DESACTUALIZADO | La última evaluación supera tres minutos de edad. | Revisar salud del pipeline antes de interpretar el riesgo. |

Hay 14 combinaciones: siete observadas y siete predictivas. BTC y SOL tienen volatilidad, caída y actividad; la divergencia es una serie conjunta. Las filas normales son evaluaciones reales y no se guardan como alertas activadas.

### Reglas y unidades

- Volatilidad observada y prevista: porcentaje de volatilidad realizada de 15 rendimientos de un minuto, sin anualizar. Referencias P95 por activo calculadas con entrenamiento: BTC aproximadamente 0,539 % y SOL 1,009 %.
- Caída observada: rendimiento de los últimos 30 minutos bajo el P5 histórico del activo. Probabilidad futura: estima el evento al finalizar los próximos 30 minutos. El umbral de caída es aproximadamente −0,558 % BTC y −1,116 % SOL; es distinto del umbral de probabilidad que activa vigilancia predictiva.
- Actividad observada: máximo de volumen/P95 y operaciones/P95 de cinco minutos. Un ratio mayor o igual a 1 supera una referencia. Las referencias dependen del activo y la hora UTC del entrenamiento. La probabilidad futura estima esa actividad en los próximos cinco minutos.
- Divergencia observada: rendimiento SOL menos BTC, en puntos porcentuales; se compara su magnitud absoluta con el umbral histórico aproximado de 0,778 puntos. El signo indica cuál rindió más.
- Correlación: Pearson entre rendimientos de un minuto en 60 observaciones consecutivas; no implica causalidad.

Las probabilidades de los gráficos se formatean como porcentajes; en la tabla detallada se muestran en escala 0–1 y con su unidad. La volatilidad utiliza porcentaje y la divergencia puntos porcentuales. Los precios están expresados en USDT.

## Datos y proceso

La fuente sigue siendo Binance Spot público, BTCUSDT y SOLUSDT. Se usan archivos oficiales mensuales de un minuto desde enero de 2022 hasta septiembre de 2026, con comprobación SHA256: 114 archivos, 4.993.759 registros de entrada y 4.993.757 registros válidos. Dos registros permanecen en cuarentena; no se fabrican velas para cerrar huecos.

El vivo utiliza `wss://data-stream.binance.vision/stream?streams=btcusdt@kline_1m/solusdt@kline_1m`, únicamente velas cerradas. REST consulta `/api/v3/time` y `/api/v3/klines` en `https://data-api.binance.vision`. No requiere claves de una cuenta Binance.

Flujo batch: archivos → validación y normalización → Parquet particionado → variables y etiquetas → entrenamiento/validación/prueba cronológicos → registro versionado.

Flujo vivo: WebSocket y recuperación REST → Kafka → Spark Structured Streaming → PostgreSQL → Grafana. Los modelos reutilizan las funciones de variables del batch. Prometheus publica el estado operativo.

La entrega conserva las cuatro dimensiones del brief y el avance funcional S01–S12. Los roles, firmas y evidencias académicas quedan fuera de esta operación del producto, conforme al alcance solicitado.

## Correcciones incorporadas

### Recuperación sin pérdida intermedia

Se separó el máximo recibido del cursor reconciliado por REST. WebSocket ya no hace avanzar ese cursor. La recuperación vuelve a consultar un tramo solapado y acepta una vela intermedia aunque ya haya llegado otra posterior. Los identificadores estables y las claves únicas de PostgreSQL evitan duplicar el registro.

Se recuperaron desde Binance las dos velas reales con `open_ms=1793232420000`, ausentes en ambos activos. La herramienta de reparación permite revisar otros huecos internos de las últimas 72 horas. Si el origen no ofrece una vela, informa el faltante y conserva la degradación; no inventa un precio.

### Variables, contrato e inferencia coherentes

Las sumas de cinco y sesenta minutos exigen continuidad, no solo una cantidad de filas. Los pares BTC/SOL solo se calculan para el ancla actual sincronizada; no se recicla un par antiguo como si fuera reciente.

El mismo contrato valida el archivo y las ventanas. Mensajes nulos de Kafka, JSON malformados, precios no finitos, valores incompatibles con PostgreSQL y tipos inválidos se rechazan sin detener por ello la consulta de archivo.

El guardado de JSON usa temporales únicos, reemplazo atómico y reintentos acotados para bloqueos transitorios de Windows.

### Señales y evaluación con trazabilidad

`market_signals` conserva valor, umbral, origen, severidad, interpretación, acción de revisión, vigencia y versión. Las señales observadas no se presentan como predicciones. El historial filtra cruces reales; el estado actual no queda vacío.

Las predicciones conservan su valor original. Al madurar, se evalúan con las reglas de su propia versión mediante `model_versions`. Las que tienen datos incompletos registran un motivo y se reintentan de forma rotativa para no bloquear otras evaluaciones maduras.

### Salud y alertas operativas reales

`/health` y `/ready` responden HTTP 503 si el producto no está listo; `/health/live` indica disponibilidad del proceso. `/api/status` mantiene el detalle del pipeline e incorpora salud y señales; `/api/signals` devuelve las evaluaciones y su estado explícito.

Se incorporaron métricas de frescura de pronósticos, continuidad y contexto, y tres reglas adicionales de Prometheus. S10/S12 comprueban las siete series recientes de la versión activa. S11 comprueba errores de consultas, incluso si Grafana devuelve HTTP 200, y exige un estado explicativo en el panel de señales.

### Evaluación temporal corregida

La calibración de los clasificadores se separa 30 minutos del inicio del tramo utilizado para seleccionar sus umbrales. El comando `training --recalibrate` reutiliza los clasificadores base y vuelve a calcular calibración, selección y métricas. Los regresores se conservan. La nueva versión solo se publica al terminar el proceso; el test evalúa y no elige el modelo.

### Operación y persistencia

El arranque aplica migraciones idempotentes sobre volúmenes existentes y publica la versión activa. Los scripts de arranque/entrenamiento ya tratan una imagen o un contenedor ausente como un estado recuperable, sin abortar prematuramente por `docker inspect`.

Código, históricos, modelos y checkpoints están en `D:\bigdata`; Kafka, PostgreSQL, Grafana y Prometheus conservan sus volúmenes administrados por Docker. Ninguna corrección requiere borrar esos volúmenes.

## Arrancar, verificar y detener

Abre Docker Desktop y espera a que el motor Linux esté disponible. Desde PowerShell:

```powershell
Set-Location D:\bigdata
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\iniciar_producto.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verificar_producto.ps1
```

Jupyter es independiente y opcional para consultar el producto:

```powershell
docker compose -f pyspark\compose.yml up -d
```

Acceso: http://localhost:4488/lab?token=sintoken.

Detener conservando datos:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\detener_producto.ps1
docker compose -f pyspark\compose.yml stop
```

Consultar logs:

```powershell
docker compose -f platform.yml logs --tail 60 producer streaming exporter
```

Reparar huecos disponibles en la fuente:

```powershell
docker compose -f platform.yml run --rm --no-deps tools python -m cryptopulse.producer --repair-gaps --hours 72
```

El resultado de esa herramienta confirma publicaciones; la verificación de producto confirma además que el sink recuperó continuidad y pronósticos.

## Pruebas reproducibles

Regresiones de ingesta, señales, salud y contrato:

```powershell
docker compose -f platform.yml run --rm --no-deps tools python -m unittest cryptopulse.test_ingestion cryptopulse.test_signals cryptopulse.test_health cryptopulse.test_stream_contract cryptopulse.test_training_calibration
```

Pruebas con Spark, modelos guardados y velas reales (sinks aislados):

```powershell
docker compose -f platform.yml run --rm --no-deps -e CRYPTOPULSE_SPARK_TESTS=1 -e 'PYSPARK_SUBMIT_ARGS=--master local[2] --driver-memory 1g pyspark-shell' tools python -m unittest cryptopulse.test_inference_spark -v
```

Integración Kafka, watermark, checkpoint y evaluación futura con replay identificado:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\pruebas_integracion.ps1
```

Interrupción controlada real del productor y recuperación automática:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\prueba_recuperacion.ps1
```

Esta última pausa únicamente el productor del proyecto durante 75 segundos, exige detectar degradación y lo restaura en un bloque `finally`. Ejecutarla interrumpe brevemente la ingesta en vivo de forma intencional; no detiene servicios ajenos.

Los resultados operativos se guardan en `pyspark/sesiones/_runtime/`, incluidos `ingestion_repair.json`, `recovery_acceptance.json`, `integration_tests.json` y `s12.json`. El archivo `aceptacion_producto.json` recoge la comprobación final de esta entrega.

## Límites que se muestran explícitamente

- Un único broker y Spark local dentro de Docker no proporcionan alta disponibilidad ni garantía de operación ininterrumpida.
- Los clasificadores pueden fallar; el de caídas ha mostrado baja precisión/recall. Las métricas vigentes aparecen en el tablero y en el registro, con referencia comparativa.
- El regresor candidato de divergencia no mejoró su referencia; el pronóstico desplegado es cero. La utilidad de esa dimensión incluye detectar divergencia observada y cambios de correlación, sin fingir capacidad predictiva adicional.
- El entrenamiento/evaluación históricos usan observaciones cada 15 minutos y el vivo cada minuto. Las métricas en vivo se presentan aparte para comprobar esa diferencia de frecuencia.
- Las ventanas finalizadas por watermark pueden tener cobertura incompleta; se muestra cuántas no contienen cinco velas. Una reparación antigua restaura el archivo y los modelos, pero no reabre automáticamente estados de ventana ya cerrados.
- Las reglas operativas se consultan en Prometheus; no se ha configurado envío externo por correo o mensajería.
- El costo es un escenario ilustrativo configurable, no una factura de Docker.

Fuentes públicas: [Binance Public Data](https://github.com/binance/binance-public-data) y [Binance Market Data Only](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md). Alcance: brief técnico CryptoPulse entregado por el usuario y guía del curso hasta S12.
