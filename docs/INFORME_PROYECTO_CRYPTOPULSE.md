> Estado histórico: este documento conserva los resultados y hallazgos de su fecha. Las correcciones y la operación vigente están en [PRODUCTO_FINAL.md](PRODUCTO_FINAL.md).

# Informe técnico y operativo de CryptoPulse

**Proyecto:** CryptoPulse, plataforma Big Data para Bitcoin y Solana.  
**Ubicación activa:** `D:\bigdata`.  
**Alcance realizado:** implementación funcional de las sesiones 1 a 12, hasta el cierre de la unidad 2.  
**Fecha del informe:** 26 de octubre de 2026, zona America/Lima.  
**Corte de la captura operativa:** 26 de octubre de 2026 a las 21:54:29, UTC−05:00; equivale al 27 de octubre a las 02:54:29 UTC.  
**Versión de modelos inspeccionada:** `20261026T223241Z`.

Este informe explica qué construye CryptoPulse, cómo se procesaron los datos, qué significan las salidas de Grafana y cómo arrancar, verificar y detener el proyecto. Se basa en el código y la configuración de `D:\bigdata`, el registro de modelos, las verificaciones de las sesiones y una consulta real de los servicios. Los contadores operativos son una fotografía de ese corte y seguirán cambiando mientras el sistema esté encendido.

El detalle del trabajo por sesión está en el documento complementario `D:\bigdata\docs\INFORME_SESIONES_S01_S12.md`.

## 1. Resultado y propósito

CryptoPulse integra datos históricos y velas cerradas en vivo de los pares Spot **BTCUSDT** y **SOLUSDT** para observar cuatro condiciones diferentes: volatilidad, riesgo de caída, actividad de negociación anómala y divergencia entre los rendimientos de ambos activos.

El producto tiene un pipeline batch reproducible, un pipeline streaming persistente y un tablero compartido. La última comprobación integral S12 volvió a producir **PASS** después del cambio de contraseña de Grafana. También se verificó que las cuatro dimensiones ya tienen predicciones en vivo cuyo resultado futuro ha podido evaluarse.

El sistema entrega señales de seguimiento. No compra ni vende criptomonedas, no genera órdenes y no promete rentabilidad. Es un laboratorio funcional local; no se ha desplegado como servicio de alta disponibilidad.

### 1.1 Pregunta central

¿Cómo integrar datos históricos y en tiempo real de BTC y SOL para detectar condiciones de riesgo y estimar su evolución a corto plazo, distinguiendo intensidad de movimiento, dirección bajista, actividad inusual y separación entre activos?

### 1.2 Alcance concreto

| Dimensión | Qué responde | Horizonte | Salidas |
|---|---|---|---|
| Volatilidad | Cuánto movimiento se espera, sin asumir una dirección | 15 minutos | Volatilidad actual, pronóstico, resultado futuro y alerta |
| Caídas | Probabilidad de que el rendimiento al cabo del horizonte esté bajo el umbral histórico del activo | 30 minutos | Probabilidad, referencia y alerta |
| Actividad anómala | Probabilidad de que el volumen o las operaciones de los próximos cinco minutos excedan la referencia histórica | 5 minutos | Probabilidad, actividad actual, percentiles y z-score |
| Divergencia | Diferencia prevista entre el rendimiento de SOL y el de BTC | 15 minutos | Diferencia reciente, pronóstico, correlación y resultado futuro |

Se generan hasta **siete salidas por cierre de minuto**: volatilidad, caídas y anomalías para cada activo, más una divergencia conjunta BTC–SOL. Las variables deben estar completas y los datos temporales necesarios deben estar disponibles.

## 2. Arquitectura implementada

La arquitectura es **Lambda**: el histórico se procesa y se usa para entrenar fuera de línea, mientras los eventos nuevos se procesan de forma continua. Ambas rutas reutilizan las definiciones de variables y convergen en datos consultables por Grafana.

~~~mermaid
flowchart TB
    Hist["Binance Public Data<br/>CSV mensuales reales"] --> Quality["PySpark<br/>calidad y normalización"]
    Quality --> Lake["Parquet por activo año y mes"]
    Lake --> Training["Variables y entrenamiento MLlib"]
    Training --> Registry["Modelos umbrales y métricas versionados"]

    Live["Binance WebSocket<br/>velas cerradas + respaldo REST"] --> Producer["Productor Python"]
    Producer --> Kafka["Kafka<br/>topics BTC y SOL"]
    Kafka --> Archive["Spark query archive<br/>validación y archivo idempotente"]
    Archive --> Candles["PostgreSQL<br/>market_candles"]
    Candles --> Features["Contexto reciente<br/>variables comunes batch y streaming"]
    Registry --> Inference["Inferencia por cierre de vela"]
    Features --> Inference
    Inference --> Predictions["PostgreSQL<br/>predictions y market_kpis"]

    Kafka --> Windows["Spark queries fixed y sliding<br/>watermark y deduplicación"]
    Windows --> WindowDB["PostgreSQL<br/>stream_windows"]
    Lake --> BatchBI["KPIs batch y métricas de modelos"]
    BatchBI --> PostgreSQL["PostgreSQL<br/>batch_kpis y model_metrics"]

    Predictions --> Grafana["Grafana<br/>tablero único"]
    PostgreSQL --> Grafana
    Exporters["Exportador CryptoPulse<br/>Kafka exporter"] --> Prometheus["Prometheus<br/>métricas y reglas"]
    Prometheus --> Grafana
~~~

Las ventanas S08 también se guardan en PostgreSQL. Los paneles de riesgo consultan principalmente `predictions` y `market_kpis`; no debe confundirse la existencia de `stream_windows` con un panel que necesariamente consulte esa tabla.

### 2.1 Componentes y versiones fijadas

| Componente | Función | Versión o configuración observada |
|---|---|---|
| Docker Desktop y Compose | Aislamiento y ejecución local | Motor 29.7.2 en la comprobación del equipo |
| Python | Productor, consultas, verificadores y módulos | 3.10 dentro de las imágenes |
| Java | JVM de Spark | OpenJDK 21 |
| Apache Spark y PySpark | ETL, variables, MLlib y streaming | 4.2.0 |
| Jupyter Notebook | Exploración y notebooks | 7.6.2; interfaz JupyterLab disponible |
| Apache Kafka | Transporte y retención de eventos | 4.3.1; un broker, tres particiones por topic de mercado |
| PostgreSQL | Archivo operativo, resultados y datos para BI | 17.6 |
| Prometheus | Captura de métricas y evaluación de reglas | v3.14.0 |
| Grafana | Tablero, consultas y visualización | 11.4.0 |
| Kafka UI | Inspección de tópicos, particiones y mensajes | Kafbat v1.5.0 |
| Kafka exporter | Métricas del broker | v1.10.0 |
| Cliente Kafka Python | Publicación y pruebas | kafka-python 3.0.12 |
| Cliente PostgreSQL Python | Persistencia | psycopg2-binary 2.9.13 |

El runtime añade los clientes Kafka y PostgreSQL a la imagen PySpark del curso. El conector oficial `spark-sql-kafka-0-10_2.13:4.2.0` y sus dependencias se descargan desde Maven y se conservan en caché local.

Spark batch utiliza `local[4]`; el proceso streaming utiliza `local[2]` y 3 GB de heap. Los notebooks iniciales usan su propia configuración local. Hay procesamiento paralelo, pero **no se ha desplegado un clúster Spark de múltiples máquinas**.

## 3. Fuentes y significado de los datos

### 3.1 Histórico

La fuente es Binance Public Data, mercado Spot, velas de un minuto en archivos ZIP mensuales.

- Activos: BTCUSDT y SOLUSDT.
- Intervalo consolidado: enero de 2022 a septiembre de 2026.
- Archivos mensuales: 114.
- Filas descargadas: **4 993 759**.
- Filas válidas finales: **4 993 757**.
- El volumen base está expresado en BTC o SOL, según el par.
- El importe negociado o `quote_asset_volume` está expresado en USDT.

Cada ZIP se comprueba contra su archivo oficial `.CHECKSUM`. También se registra el hash del CSV extraído y se vuelve a comprobar antes de su lectura para S03. El manifiesto conserva URLs, hashes, cantidad de filas, mes y unidad temporal.

### 3.2 Flujo en vivo

El productor se suscribe a los streams públicos `btcusdt@kline_1m` y `solusdt@kline_1m`. Solo publica una vela cuando el mensaje confirma que ha cerrado. Las actualizaciones parciales no se tratan como velas finales.

El acceso WebSocket y REST es público: no se configuró una clave privada de Binance. REST se usa para recuperar contexto reciente y completar eventos omitidos durante una desconexión; no produce datos simulados.

### 3.3 Campos principales

| Campo | Significado |
|---|---|
| `symbol` | Par de mercado |
| `open_ms` | Inicio de vela en milisegundos UTC |
| `close_ms` | Fin de vela en milisegundos UTC |
| `open, high, low, close` | Apertura, máximo, mínimo y cierre en USDT |
| `volume` | Cantidad negociada del activo base |
| `quote_asset_volume` | Importe negociado en USDT |
| `number_of_trades` | Cantidad de operaciones |
| `event_id` | Identificador determinista del par, intervalo e inicio de vela |
| `source_mode` | `live`, `backfill` o `replay` |
| `transport` | WebSocket, REST o replay |
| `published_ms` | Momento de publicación del productor |
| `is_closed` | Confirmación de cierre |
| `synthetic` | En el contrato de mercado utilizado, `false` |

### 3.4 Normalización temporal

Los archivos históricos Spot anteriores a 2025 usan milisegundos y los de 2025 en adelante usan microsegundos. La transformación usa el mes de procedencia para identificar la escala y convierte a `open_ms` y `close_ms`.

El sistema trabaja en UTC. El tablero también está configurado en UTC. Para interpretar sus horas en Perú, se restan cinco horas. Por ejemplo, 02:00 UTC del día siguiente corresponde a 21:00 del día anterior en Lima.

## 4. Proceso batch y calidad

El recorrido real fue descargar, verificar procedencia, leer con esquema explícito, clasificar registros, guardar cuarentena, resolver duplicados, identificar huecos y escribir el dataset válido.

### 4.1 Reglas de calidad

Se comprueban campos obligatorios, símbolos admitidos, precios positivos, coherencia OHLC, actividad no negativa, inicio alineado a minuto y duración canónica. La duración normalizada entre inicio y cierre es 59 999 milisegundos.

Si una clave temporal tiene valores de mercado incompatibles, la política excluye esas versiones y las conserva para inspección. Los duplicados compatibles se reducen a una vela por `symbol + open_ms`. En la ejecución observada no hubo claves conflictivas ni filas retiradas por duplicación.

### 4.2 Resultado real

| Resultado | Cantidad |
|---|---:|
| Entrada | 4 993 759 |
| Registros válidos | 4 993 757 |
| Registros en cuarentena | 2 |
| Claves con versiones conflictivas | 0 |
| Filas retiradas por duplicados o conflictos | 0 |
| Huecos temporales después de calidad | 3 |
| Minutos ausentes, sumados entre activos | 163 |

Los dos registros en cuarentena corresponden a BTCUSDT y SOLUSDT con inicio **2023-03-24 12:39 UTC**. La regla que no superaron fue `invalid_interval`. Sus precios no se describen como nulos o erróneos: el motivo identificado fue el intervalo temporal.

Los huecos se desglosan así:

| Activo | Inicio de la siguiente vela disponible | Minutos ausentes |
|---|---|---:|
| SOLUSDT | 2023-02-14 16:40 UTC | 1 |
| BTCUSDT | 2023-03-24 14:00 UTC | 81 |
| SOLUSDT | 2023-03-24 14:00 UTC | 81 |

Los 163 minutos son una suma por activo, no necesariamente 163 minutos distintos del calendario. La cifra incluye el efecto de excluir los dos intervalos inválidos. No se rellenan huecos con precios inventados.

### 4.3 Almacenamiento

La salida válida se conserva en:

`D:\bigdata\pyspark\sesiones\s03-calidad\artifacts\clean_parquet`

Se particiona por `symbol/year/month`. Las consultas por activo y periodo pueden omitir carpetas ajenas al filtro. La verificación encontró `PartitionFilters` en el plan físico y comprobó una lectura filtrada real.

La cuarentena, el detalle de huecos y el informe JSON de calidad quedan en la misma carpeta de artefactos de S03.

## 5. Variables y objetivos de aprendizaje automático

Las funciones compartidas están en `D:\bigdata\pyspark\sesiones\cryptopulse\features.py`. Se utilizan tanto en batch como en streaming para mantener la misma definición de predictores.

### 5.1 Variables retrospectivas

| Grupo | Variables o criterio |
|---|---|
| Rendimiento | Cambios de 1, 5, 15 y 30 minutos; solo cuando coincide el intervalo temporal |
| Volatilidad | Volatilidad realizada de 15 y 60 minutos |
| Actividad | Importe y operaciones de ventanas recientes, transformación `log1p` |
| Actividad relativa | Relación entre la actividad media de cinco minutos y la de sesenta |
| Vela | Rango porcentual y cuerpo porcentual |
| Estacionalidad | Seno y coseno de la hora UTC |
| Identificación | Indicador de SOL frente a BTC |
| Relación entre activos | Variables de ambos, correlación móvil y spread reciente |

Los precios originales se conservan con tipo decimal. Para rendimientos y cocientes derivados se convierten los operandos a doble precisión **antes** de dividir, evitando un redondeo prematuro de la aritmética decimal.

La volatilidad se calcula como:

~~~text
r_t = ln(cierre_t / cierre_(t-1))
RV_15 = 100 × raíz( suma de r_t² de 15 retornos consecutivos )
~~~

Es una magnitud porcentual sin anualizar. No es la misma definición que una desviación estándar anualizada.

Una ventana de volatilidad de 15 retornos necesita 16 cierres. Las funciones comprueban continuidad temporal; no convierten un salto de varios minutos en un rendimiento de un minuto. Si la correlación tiene varianza cero y no se puede definir, se conserva como dato no disponible.

### 5.2 Objetivos futuros

- **Volatilidad:** volatilidad realizada de los siguientes 15 retornos.
- **Caídas:** rendimiento final de 30 minutos bajo el percentil 5 del entrenamiento por activo.
- **Anomalías:** importe o número de operaciones de los siguientes cinco minutos sobre el percentil 95 de entrenamiento del activo y hora UTC correspondientes.
- **Divergencia:** rendimiento futuro de SOL a 15 minutos menos el de BTC al mismo horizonte, en puntos porcentuales.

Las etiquetas futuras se calculan para entrenamiento y evaluación offline. No se incorporan como predictores del flujo en vivo.

## 6. Entrenamiento selección y evaluación

### 6.1 Separación cronológica

| Uso | Periodo |
|---|---|
| Entrenamiento | 2022–2024 |
| Validación | 2025 |
| Ajuste del calibrador de clasificadores | Enero–agosto de 2025 |
| Selección y umbrales de alerta de clasificadores | Septiembre–diciembre de 2025 |
| Prueba final | Enero–septiembre de 2026 |

Hay un embargo explícito de 30 minutos en los límites entrenamiento/validación y validación/prueba. No se aplica una división aleatoria para evaluar las series. La subdivisión interna de calibración y selección en 2025 se hace por fecha; el código no añade un embargo separado en ese límite interno.

Las variables y etiquetas se calculan sobre velas de un minuto. Para ajustar y evaluar modelos se toman observaciones cada 15 minutos; en operación la inferencia se calcula en cada cierre de un minuto.

### 6.2 Tamaños supervisados

| Dataset | Entrenamiento | Validación | Prueba |
|---|---:|---:|---:|
| Dimensiones por activo, BTC y SOL conjuntamente | 210 392 | 70 076 | 52 412 |
| Divergencia del par sincronizado | 105 195 | 35 038 | 26 207 |

Los tamaños son menores que el histórico porque requieren contexto previo, objetivos futuros disponibles, sincronización, separación temporal y muestreo por tiempo.

### 6.3 Candidatos y política de selección

Para volatilidad se compararon regresión lineal regularizada, Random Forest y persistencia de la volatilidad actual. Ganó Random Forest.

Para caídas y actividad anómala se entrenó regresión logística con ensamblado de variables y escalado. Se ajustó una calibración de tipo Platt sobre probabilidades del periodo de calibración. La decisión de desplegar usa Brier en selección frente a la frecuencia histórica por activo.

Para divergencia se comparó un regresor lineal regularizado con el pronóstico cero. La selección usa RMSE. El candidato no mejoró la referencia y se desplegó el baseline.

Los umbrales de alerta de clasificadores se seleccionan en validación por F1. El conjunto de prueba no interviene en la selección del método. Los candidatos, incluidos los no seleccionados, quedan guardados.

### 6.4 Métricas de prueba

Las tablas usan punto decimal. MAE y RMSE de las regresiones están en puntos porcentuales de su variable objetivo.

| Dimensión | Método desplegado | Casos | MAE | RMSE | RMSE de la referencia |
|---|---|---:|---:|---:|---:|
| Volatilidad | random_forest | 52 412 | 0.078744 | 0.129191 | 0.150802 |
| Divergencia | baseline | 26 207 | 0.131394 | 0.199551 | 0.199551 |

| Dimensión | Casos | PR-AUC | Brier | Precisión | Recall | ECE |
|---|---:|---:|---:|---:|---:|---:|
| Caídas | 52 412 | 0.103077 | 0.025002 | 13.54 % | 26.21 % | 0.003102 |
| Actividad anómala | 52 412 | 0.659547 | 0.039133 | 56.93 % | 63.60 % | 0.011731 |

**Interpretación.** Menores MAE, RMSE y Brier son mejores para sus respectivos problemas. PR-AUC mide capacidad de ordenar casos positivos; no debe leerse como porcentaje de aciertos. ECE resume diferencias entre probabilidad media y frecuencia observada por grupos, sin garantizar calibración perfecta.

El clasificador de caídas tiene una capacidad limitada al umbral seleccionado: en prueba detectó 362 positivos, con 2 312 falsos positivos y 1 019 falsos negativos. El clasificador de anomalías obtuvo 2 362 positivos detectados, 1 787 falsos positivos y 1 352 falsos negativos. Estas cifras ayudan a interpretar las señales y evitan presentar probabilidades como certezas.

### 6.5 Por qué la divergencia aparece en cero

En validación, el RMSE del baseline fue **0.321614** y el del candidato Ridge **0.321911**. Por la regla fijada, se conservó cero. En prueba, sus RMSE fueron **0.199551** y **0.199867**, respectivamente.

El cero es el pronóstico de referencia de la diferencia futura, no la afirmación de que ambos activos tengan el mismo precio, la misma evolución o un riesgo nulo. La correlación y el spread reciente siguen siendo indicadores observados diferentes.

### 6.6 Umbrales guardados

| Regla | BTCUSDT | SOLUSDT |
|---|---:|---:|
| Caída significativa a 30 minutos | Rendimiento ≤ −0.557740 % | Rendimiento ≤ −1.116490 % |
| Alerta por volatilidad prevista a 15 minutos | ≥ 0.539071 % | ≥ 1.009327 % |

- Probabilidad de caída que activa alerta: aproximadamente **5.8009 %**.
- Probabilidad de anomalía que activa alerta: aproximadamente **34.2464 %**.
- Umbral absoluto de divergencia: aproximadamente **0.777720 puntos porcentuales**.
- Los percentiles de actividad se conservan por activo y por cada una de las 24 horas UTC.

El umbral de probabilidad de caída y el umbral de rendimiento que define una caída son cantidades distintas.

## 7. Proceso en tiempo real

### 7.1 Secuencia de un evento

1. Binance comunica la vela de un minuto.
2. El productor espera la confirmación de cierre.
3. Valida el contrato y asigna un identificador estable.
4. Publica en el topic del activo con clave `BTCUSDT` o `SOLUSDT` y espera confirmación de Kafka.
5. Spark lee el evento y el archivo operativo se inserta de forma idempotente.
6. Se recupera el contexto reciente de ambos activos desde PostgreSQL.
7. Se calculan las variables con las mismas funciones usadas en entrenamiento.
8. Se aplica la versión de modelos cargada; no se reentrena por evento.
9. Se guardan pronósticos, indicadores y alertas.
10. Grafana consulta la base y Prometheus recoge métricas de operación.
11. Cuando termina el horizonte, se intenta calcular la verdad futura usando las velas completas y consecutivas disponibles.

El productor dispone de recuperación REST y reconexión con espera creciente. Distingue `live`, `backfill` y `replay`. Una recuperación puede incorporar velas reales antiguas; eso no implica que se hayan emitido en vivo todas las predicciones correspondientes a un periodo de interrupción.

### 7.2 Tópicos y contrato

- `market.klines.btcusdt`.
- `market.klines.solusdt`.
- `cryptopulse.integration`, para pruebas de publicación y consumo.
- Tópicos `cryptopulse.integration.<identificador>`, usados por pruebas aisladas.
- `market.deadletter` está creado, pero el rechazo implementado por el consumidor se persiste en la tabla `rejected_events`; no se afirma que todos los rechazos se publiquen en ese tópico.

Los topics de mercado tienen tres particiones y el despliegue tiene un broker. Con un símbolo como clave dentro de un topic dedicado a ese símbolo, sus eventos suelen concentrarse en una partición; tres particiones no significan que ese activo se procese en tres flujos independientes.

El esquema está en `D:\bigdata\schemas\market-kline-v1.schema.json` y la validación ejecutable en `D:\bigdata\pyspark\sesiones\cryptopulse\contract.py`.

### 7.3 Consultas Spark y garantías

| Consulta | Función | Estado o garantía |
|---|---|---|
| `cryptopulse_archive` | Archivo, validación, inferencia y evaluación | Claves únicas en PostgreSQL y checkpoint de offsets |
| `cryptopulse_fixed` | Ventanas fijas de cinco minutos | Watermark y deduplicación temporal |
| `cryptopulse_sliding` | Ventanas de cinco minutos con paso de uno | Watermark y deduplicación temporal |

El trigger configurado es de 15 segundos. No garantiza por sí solo una latencia de 15 segundos: el tiempo real depende del procesamiento y de los servicios. Las velas de mercado se cierran cada minuto.

El watermark es de dos minutos y depende del máximo tiempo de evento observado, no de un reloj que cierre ventanas cuando no llegan eventos. Una ventana final se emite después de superar su cierre según el watermark.

El archivo operativo puede aceptar un evento válido muy tardío aunque las consultas de ventanas ya lo excluyan. Las predicciones emitidas se conservan por clave y versión; no se sobreescriben retrospectivamente como si hubieran conocido datos que llegaron después.

La garantía implementada es entrega al menos una vez con deduplicación y escrituras idempotentes. No hay una transacción distribuida global entre Kafka, Spark y PostgreSQL.

## 8. Persistencia y trazabilidad

| Tabla | Contenido | Clave principal |
|---|---|---|
| `market_candles` | Velas operativas y procedencia | Activo e inicio |
| `predictions` | Pronóstico, baseline, observado, alerta y resultado futuro | Dimensión, activo, emisión y versión |
| `market_kpis` | Variables observadas, percentiles y correlación | Activo y emisión |
| `stream_windows` | Agregaciones finales fijas y deslizantes | Tipo, activo e inicio de ventana |
| `model_metrics` | Métricas offline del candidato, baseline y método desplegado | Dimensión, partición, método y versión |
| `batch_kpis` | Resumen diario del histórico | Activo y fecha |
| `stream_batches` | Filas y duración de micro-lotes | Consulta y batch |
| `rejected_events` | Payloads rechazados con su causa | Hash del evento |

Los datos batch, los modelos, el código y los checkpoints se encuentran en `D:\bigdata`. Kafka, PostgreSQL, Prometheus y Grafana usan volúmenes administrados por Docker; su ubicación física depende del disco virtual de Docker Desktop, no solo de la carpeta del proyecto.

## 9. Qué muestra Grafana

El tablero tiene **19 paneles**. PostgreSQL aporta datos de negocio y de evaluación. Prometheus aporta métricas operativas. La actualización está configurada cada diez segundos y la zona es UTC.

### 9.1 Lectura de los paneles

| Panel | Interpretación |
|---|---|
| Histórico validado | Número de velas del batch publicado; es estático hasta una nueva publicación |
| Último cierre BTCUSDT | Cierre de la última vela almacenada, no un precio tick a tick |
| Último cierre SOLUSDT | Mismo criterio para SOL |
| Antigüedad de la última vela | Diferencia entre el momento actual y el último cierre disponible |
| Volatilidad | Compara variable actual, pronóstico a 15 minutos y verdad futura cuando madura |
| Probabilidad de caída | Probabilidad del evento definido para el rendimiento final a 30 minutos |
| Probabilidad de anomalía | Pronóstico a cinco minutos y bandera de anomalía actual |
| Divergencia | Spread reciente, pronóstico y valor futuro observado; actualmente usa baseline cero |
| Correlación | Relación móvil de rendimientos en 60 minutos; no prueba causalidad |
| Alertas de mercado | Predicciones que superaron su regla; una tabla vacía puede significar ausencia de señales |
| Evaluación histórica | Métricas sobre la prueba cronológica de 2026, con método desplegado |
| Evaluación en vivo | Error calculado solo para predicciones cuyo resultado futuro está disponible |
| WebSocket conectado | Indicador reportado por el productor |
| Edad de heartbeat Spark | Tiempo desde la actualización de estado del driver |
| Kafka pendiente | Suma de offsets pendientes según broker y progreso de Spark |
| Costo ilustrativo mensual | Estimación configurable, no factura |
| Duración de micro-lotes | Tiempos de ejecución por consulta |
| Mensajes por topic | Offsets actuales reportados por Kafka exporter; no es por sí solo una tasa por segundo |
| Actividad y umbrales | Importe y operaciones de cinco minutos, percentiles históricos y z-score |

Los puntos de pronóstico se sitúan en el momento de emisión de la vela, aunque su horizonte sea futuro. La línea de resultado futuro se completa sobre ese mismo punto cuando la verdad se conoce; así puede compararse pronóstico con resultado.

El panel de evaluación en vivo combina dimensiones con métricas de distinta naturaleza: en regresión se interpreta MAE/RMSE; en clasificación, el error cuadrático medio es Brier y su raíz no debe llamarse RMSE de precio. Las magnitudes de dimensiones diferentes no se comparan directamente.

### 9.2 Cómo distinguir observación predicción y evaluación

- **Observado:** una variable calculada con datos ya disponibles.
- **Pronóstico:** valor emitido para un horizonte que todavía no se había cumplido al emitirlo.
- **Resultado futuro:** valor real incorporado posteriormente.
- **Métrica offline:** error en el periodo de prueba histórico.
- **Métrica en vivo:** error de las predicciones realmente emitidas y ya evaluables.
- **Replay:** prueba con datos reales pasados; no se cuenta como operación en vivo.

Un dato vacío puede indicar calentamiento, horizonte todavía abierto o falta de contexto temporal completo. Si los paneles permanecen vacíos después del calentamiento, deben revisarse los servicios y la API de estado.

## 10. Observabilidad alertas y costos

Prometheus recoge el exportador de CryptoPulse, Kafka exporter y sus propias métricas. El exportador publica estado de conexión, heartbeats, conteos, duración de lotes, filas descartadas por watermark y diferencia entre offsets finales del broker y los procesados por Spark.

Las cinco reglas configuradas son:

| Regla | Condición | Persistencia antes de activarse |
|---|---|---|
| Exportador no disponible | Target CryptoPulse abajo | 1 minuto |
| Mercado desactualizado | Última vela con más de 180 segundos de antigüedad | 1 minuto |
| Streaming detenido | Heartbeat Spark con más de 60 segundos | 1 minuto |
| Kafka con retraso | Más de 100 eventos pendientes | 2 minutos |
| WebSocket sin mensajes | Más de 90 segundos sin mensaje | 1 minuto |

Las reglas se consultan en Prometheus. Las alertas de negocio se conservan en predicciones y se muestran en Grafana. No se configuró un canal externo de notificación por correo, Telegram o Slack.

### 10.1 Estimación de costos

La estimación ilustrativa usa:

~~~text
costo mensual =
horas × (núcleos × tarifa por núcleo/hora + RAM GiB × tarifa por GiB/hora)
+ almacenamiento GiB × tarifa por GiB/mes
~~~

Los supuestos actuales son 730 horas, 3.5 núcleos, 5.5 GiB de RAM y 20 GiB de disco. Las tarifas ilustrativas son 0.02, 0.003 y 0.10 USD en sus unidades correspondientes. El cálculo produce **65.145 USD/mes**.

Son parámetros de un escenario, no una medición facturada, una cotización de nube o el costo eléctrico real del equipo. Se editan en `D:\bigdata\config\cost-assumptions.json` y se recalculan con `D:\bigdata\scripts\actualizar_costos.ps1`.

## 11. Estado operativo comprobado

La captura de 21:54:29 de Lima contenía:

| Dimensión | Predicciones live guardadas | Con resultado futuro | Todavía sin resultado |
|---|---:|---:|---:|
| anomaly | 206 | 196 | 10 |
| crash | 206 | 146 | 60 |
| divergence | 103 | 88 | 15 |
| volatility | 206 | 176 | 30 |

Los contadores incluyen ambos activos en las dimensiones individuales. El menor número de divergencias se debe a que se guarda una salida conjunta por momento.

También se observaron 172 ventanas fijas, 860 deslizantes, 3 468 filas de resumen diario batch y cero eventos rechazados persistidos. Cada activo tenía 110 velas etiquetadas `live`, 324 `backfill` y 190 `replay`. Todas proceden de datos reales; su modo describe el contexto de uso, no una simulación de precios.

Las pruebas aisladas conservaron siete predicciones replay, todas evaluadas: dos de volatilidad, dos de caídas, dos de anomalía y una de divergencia. Estas filas no se mezclan con los paneles de predicción live.

### 11.1 Comprobaciones realizadas

- Login de Grafana con la contraseña nueva y rechazo de la anterior.
- Comprobación de los servicios y fuentes de Grafana.
- Ejecución real de las 13 consultas de paneles PostgreSQL.
- Targets Prometheus activos y cinco reglas cargadas.
- Frescura de las velas y ausencia de duplicados persistidos.
- Contrato válido y seis variantes inválidas rechazadas.
- Duplicado contado una sola vez en el stream de prueba.
- Evento anterior al watermark descartado.
- Reinicio de la consulta aislada con recuperación de checkpoint y sink.
- Evaluación de las cuatro dimensiones con replay real.
- Modificación de datos futuros sin alteración de las variables del pasado.
- Inserción repetida de pronósticos sin crear duplicados.

`PASS` significa que esas comprobaciones funcionales terminaron correctamente. No equivale a una nota académica, una garantía de rentabilidad ni una prueba de carga de producción.

## 12. Guía para levantar el proyecto en este equipo

### 12.1 Requisitos

- Carpeta del proyecto en `D:\bigdata`.
- Docker Desktop abierto y usando contenedores Linux.
- PowerShell.
- Conexión a Internet para datos públicos y descargas iniciales.
- Puertos de acceso disponibles.
- Memoria suficiente para el laboratorio. El motor inspeccionado dispone de aproximadamente 12 GiB; batch y streaming tienen presupuestos de memoria separados.

Python y Java están dentro de las imágenes. Para los comandos documentados no es necesario instalarlos en Windows.

### 12.2 Arranque habitual

~~~powershell
Set-Location D:\bigdata
docker version

powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\iniciar_producto.ps1
~~~

El script comprueba Docker y el registro de modelos, crea `.env` desde el ejemplo si falta, construye imágenes ausentes y levanta la plataforma. Los modelos y datos de este equipo ya existen: **no hay que reentrenar cada vez que se arranca**.

Después de un arranque frío, espera al calentamiento de Spark y a que llegue un cierre de vela. El tablero consulta cada diez segundos, pero las velas se actualizan por minuto.

### 12.3 Entrar a las interfaces

| Interfaz | URL | Acceso |
|---|---|---|
| Grafana | http://localhost:43000/d/cryptopulse-main | Usuario `admin` y contraseña `admin2026!` |
| Kafka UI | http://localhost:48085 | Acceso local |
| Prometheus | http://localhost:49090 | Acceso local |
| Estado JSON | http://localhost:48000/api/status | Acceso local |
| JupyterLab | http://localhost:4488/lab?token=sintoken | Token `sintoken` |
| Spark UI del notebook | http://localhost:4042 | Solo si hay una SparkSession activa en el puerto interno expuesto |

La Spark UI de la tabla corresponde al entorno de notebooks, no necesariamente al proceso streaming. Cuando varios kernels crean SparkSession, pueden ocupar puertos internos adicionales.

### 12.4 Levantar los notebooks

La plataforma y el servicio de notebooks se gestionan por separado:

~~~powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\iniciar_s01.ps1
~~~

El script espera que esté disponible la muestra inicial S01. En este equipo está incluida. Para trabajar con S02, el notebook ejecutado se encuentra en `D:\bigdata\pyspark\sesiones\s02-fundamentos\artifacts\02_fundamentos_practica_ejecutado.ipynb`.

### 12.5 Verificar el producto

~~~powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verificar_producto.ps1
~~~

El cierre se guarda en `D:\bigdata\pyspark\sesiones\_runtime\s12.json`. El comando comprueba el producto en funcionamiento y usa también el resultado de las pruebas de integración guardadas; no vuelve a entrenar ni a ejecutar todos los escenarios de prueba.

Para volver a ejecutar esos escenarios:

~~~powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\pruebas_integracion.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verificar_producto.ps1
~~~

### 12.6 Inspeccionar servicios y logs

~~~powershell
docker compose -f .\platform.yml ps
docker compose -f .\platform.yml logs --tail 100 producer
docker compose -f .\platform.yml logs --tail 100 streaming
docker compose -f .\platform.yml logs --tail 100 exporter
docker compose -f .\platform.yml logs --tail 100 grafana

Invoke-RestMethod http://localhost:48000/api/status
~~~

Para seguir el consumidor mientras trabaja:

~~~powershell
docker compose -f .\platform.yml logs -f --tail 30 streaming
~~~

`Ctrl+C` deja de seguir los logs; no detiene por sí mismo el contenedor.

### 12.7 Detener y volver a iniciar

~~~powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\detener_producto.ps1
~~~

Ese script detiene solo la plataforma CryptoPulse y conserva volúmenes, históricos, modelos y checkpoints. Los notebooks se detienen por separado:

~~~powershell
docker compose -f .\pyspark\compose.yml stop
~~~

Para volver, ejecuta los comandos de inicio. No se necesita eliminar los volúmenes para un reinicio normal.

## 13. Reconstrucción y actualización del batch

### 13.1 Cambiar el intervalo

El rango de descarga está en `D:\bigdata\config\pipeline.json`. El corte actual es `2022-01` a `2026-09`. El descargador admite meses cerrados y verifica los archivos disponibles; un archivo ausente no se sustituye con datos inventados.

Las fechas de entrenamiento, validación y calibración están definidas en `D:\bigdata\pyspark\sesiones\cryptopulse\core.py`. Cambiar el rango de descarga no modifica automáticamente esos límites de evaluación.

### 13.2 Descargar aplicar calidad y entrenar

~~~powershell
Set-Location D:\bigdata
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\entrenar_producto.ps1 -Descargar -Reprocesar -Reentrenar
~~~

El script reutiliza el contenedor de notebooks. Si el streaming está activo, pausa ese consumidor para dejar memoria al entrenamiento y lo restaura al terminar. El productor sigue acumulando eventos en Kafka.

La preparación de variables se reutiliza cuando su firma y las salidas completas coinciden. El registro de modelos se publica de manera atómica cuando termina el entrenamiento y la evaluación; una carpeta de modelo incompleta no se selecciona como registro nuevo.

### 13.3 Publicar resultados batch y repetir comprobaciones

~~~powershell
docker compose -f .\platform.yml run --rm --no-deps tools python -m cryptopulse.publish_batch

foreach ($sesion in 6..11) {
    docker compose -f .\platform.yml run --rm --no-deps tools python -m cryptopulse.verify $sesion
    if ($LASTEXITCODE -ne 0) {
        throw "La sesión $sesion no superó la comprobación"
    }
}

powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\pruebas_integracion.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verificar_producto.ps1
~~~

La secuencia también sirve para regenerar los registros funcionales de las sesiones streaming cuando falten. Los servicios deben estar operativos y haber completado su calentamiento.

### 13.4 Si se parte de archivos de código sin imágenes ni dataset

Esta es una secuencia de reconstrucción, no un comando necesario para el arranque diario. No borra los volúmenes existentes:

~~~powershell
Set-Location D:\bigdata

docker compose -f .\pyspark\compose.yml build
docker compose -f .\pyspark\compose.yml up -d

Get-Content .\scripts\descargar_muestra_s01.py -Raw |
    docker exec -e CRYPTOPULSE_DATA_DIR=/opt/s01-arquitectura/data -i cryptopulse-pyspark python -

powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\preparar_s02.ps1 -Desde 2022-01 -Hasta 2026-09

docker exec -e PYTHONPATH=/opt cryptopulse-pyspark python -m cryptopulse.quality
docker exec -e PYTHONPATH=/opt cryptopulse-pyspark python -m cryptopulse.training
docker exec -e PYTHONPATH=/opt cryptopulse-pyspark python -m cryptopulse.training --verify-u1

docker compose -f .\platform.yml build tools
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\iniciar_producto.ps1

docker compose -f .\platform.yml run --rm --no-deps tools python -m cryptopulse.publish_batch
~~~

Después del calentamiento, ejecuta las comprobaciones de 6 a 11 y las pruebas de integración indicadas arriba. En un equipo nuevo, las tablas batch de PostgreSQL se llenan mediante `publish_batch`; copiar modelos no llena por sí mismo esos volúmenes.

La descarga y el entrenamiento completos consumen más tiempo y recursos que el arranque habitual. La reconstrucción documenta el orden de las dependencias; la validación de este informe corresponde al entorno existente, no a una segunda instalación ejecutada sobre volúmenes vacíos.

## 14. Problemas frecuentes

| Síntoma | Comprobación y acción |
|---|---|
| Docker muestra cliente pero no servidor | Abrir Docker Desktop y esperar al motor Linux |
| Acceso denegado desde una sesión restringida de Codex | Autorizar la ejecución requerida; comprobar Docker desde PowerShell del usuario |
| Grafana no acepta la contraseña | Usar `admin/admin2026!`, cerrar una sesión antigua si procede y revisar logs |
| Se cambió `.env` pero un usuario existente sigue con otra contraseña | El password inicial de entorno no sustituye automáticamente la contraseña ya almacenada; este cambio se realizó también con la CLI de Grafana |
| Gráficas sin predicciones al arrancar | Esperar contexto suficiente y un cierre; revisar el productor, Spark y `INFERENCE_ENABLED=true` |
| No hay errores de predicción de 30 minutos todavía | El resultado solo se conoce al madurar el horizonte y disponer de las velas necesarias |
| Divergencia prevista siempre cero | Es el baseline seleccionado; consultar la tabla de evaluación para verificar el método |
| No cambia una gráfica de inmediato | Los cierres son de un minuto; refrescar Grafana no crea un cierre nuevo |
| Falta actividad por desconexión | Revisar WebSocket y recuperación REST; la recuperación no garantiza reconstruir todas las predicciones no emitidas |
| Un puerto ya está ocupado | Identificar el servicio con `docker ps` y revisar el mapeo del proyecto |
| Primer arranque streaming tarda más | Puede estar descargando el conector Spark–Kafka y sus dependencias |
| Falta una comprobación S06–S11 | Ejecutarla con `cryptopulse.verify` después del calentamiento |
| Error durante un nuevo entrenamiento | Revisar logs; el registro anterior no se reemplaza hasta completar el nuevo entrenamiento |

El proyecto no detiene globalmente otros contenedores de Docker para resolver conflictos de puertos.

## 15. Organización de archivos

~~~text
D:\bigdata
├── .env
├── .env.example
├── README.md
├── platform.yml
├── config
│   ├── pipeline.json
│   └── cost-assumptions.json
├── database
│   └── init.sql
├── docs
│   ├── INFORME_PROYECTO_CRYPTOPULSE.md
│   └── INFORME_SESIONES_S01_S12.md
├── observability
│   ├── prometheus.yml
│   ├── alerts.yml
│   └── grafana
│       ├── provisioning
│       └── dashboards
├── runtime
│   └── Dockerfile
├── schemas
│   └── market-kline-v1.schema.json
├── scripts
│   ├── iniciar_producto.ps1
│   ├── detener_producto.ps1
│   ├── verificar_producto.ps1
│   ├── entrenar_producto.ps1
│   └── pruebas_integracion.ps1
└── pyspark
    ├── compose.yml
    ├── Dockerfile
    └── sesiones
        ├── cryptopulse
        ├── s01-arquitectura
        ├── s02-fundamentos
        ├── s03-calidad
        ├── s04-modelos
        └── _runtime
~~~

La copia antigua de Documents/Codex queda como respaldo de etapas previas. Los contenedores y configuraciones activos del producto usan `D:\bigdata`.

## 16. Cambios de contraseña y validación del acceso

La contraseña de Grafana quedó en **admin2026!** para el usuario **admin**. Se actualizó la contraseña almacenada en la base interna de Grafana mediante su CLI y se sincronizaron `.env`, `.env.example`, el valor por defecto de Compose, el verificador y las instrucciones.

Se recreó únicamente Grafana conservando su volumen. La API autenticada devolvió acceso correcto con la contraseña nueva y rechazó la anterior. El producto completo volvió a pasar S12 después de este cambio.

La modificación no cambia el token de JupyterLab: sigue siendo **sintoken**.

## 17. Límites del alcance alcanzado

1. Es una implementación local en contenedores, con un broker Kafka y Spark local; no hay alta disponibilidad ni clúster de varias máquinas.
2. La validación es por periodos fijos. No se implementó validación walk-forward ni un estudio de múltiples ciclos de reentrenamiento.
3. Los ejemplos de entrenamiento y prueba se toman cada 15 minutos; su distribución no equivale exactamente a la de todas las emisiones de un minuto.
4. Los horizontes se solapan en parte. Las observaciones no deben interpretarse como ensayos estadísticos independientes.
5. El clasificador de caídas tiene recall y precisión limitados al umbral seleccionado.
6. La calibración se ajustó en histórico y puede degradarse con un cambio de régimen.
7. El modelo de divergencia no se desplegó porque no mejoró la referencia.
8. Un hueco puede impedir el cálculo de una variable o de una verdad futura; no se inventa ese dato.
9. Hay reglas y tablas de alertas, pero no canales externos de notificación.
10. La sesión 7 se adaptó a telemetría financiera y no constituye una implementación de sensores físicos ni MQTT.
11. El costo mostrado es ilustrativo.
12. Se completó el alcance funcional solicitado hasta S12. No se presenta este estado como una evaluación académica ni como el cierre de las sesiones 13 a 16.

## 18. Fuentes y evidencia

### Material base

- Brief técnico analítico CryptoPulse proporcionado por el equipo.
- [Guía del curso](https://262bigdata.github.io/lambda26/).
- [Repositorio de referencia lambda26](https://github.com/262bigdata/lambda26), commit consultado `973ac3020b573a7c66fd925f7d07a9f2553411ec`.
- [Binance Public Data](https://github.com/binance/binance-public-data).
- [Binance Market Data Only](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md).
- [Integración oficial Spark Structured Streaming con Kafka](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html).

### Evidencia local inspeccionada

- `D:\bigdata\pyspark\sesiones\s02-fundamentos\data\provenance.json`.
- `D:\bigdata\pyspark\sesiones\_runtime\s03.json` hasta `s12.json`.
- `D:\bigdata\pyspark\sesiones\s04-modelos\artifacts\model_registry.json`.
- `D:\bigdata\pyspark\sesiones\_runtime\integration_tests.json`.
- `D:\bigdata\pyspark\sesiones\_runtime\report_snapshot.json`.
- `D:\bigdata\pyspark\sesiones\_runtime\report_evidence.json`.
- `D:\bigdata\observability\grafana\dashboards\cryptopulse.json`.
- Código de `D:\bigdata\pyspark\sesiones\cryptopulse` y scripts de operación.

Las cifras históricas se atribuyen al corte de entrenamiento y calidad indicado. Los contadores live pertenecen a la captura operativa y no deben copiarse como valores permanentes del sistema.
