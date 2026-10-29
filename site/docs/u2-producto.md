# Producto de Unidad 2 · CryptoPulse

<span class="cp-tag">S06–S12 · Ingesta · Streaming · Observabilidad · Inferencia · BI</span>

## Producto

CryptoPulse integra eventos reales de BTCUSDT y SOLUSDT en Binance Spot, Kafka, Spark Structured Streaming, PostgreSQL, Prometheus y Grafana. Reutiliza modelos entrenados en batch para actualizar siete series predictivas y catorce evaluaciones explicadas tras las velas cerradas.

El resultado permite seguir volatilidad, caídas, actividad inusual y divergencia; comprueba frescura/continuidad antes de interpretar señales. No ejecuta órdenes. El pronóstico de divergencia desplegado es la referencia cero porque el candidato no ganó en validación.

![Producto U2 y su relación con el batch](assets/images/arquitectura.svg)

*La capa de velocidad reutiliza modelos del histórico y converge en la base de servicio.*

## 1. Alcance de capacidades por sesión

| Sesión | Capacidad implementada | Cómo comprobarla |
|---|---|---|
| S06 | Eventos de mercado bajo contrato v1, publicación confirmada y lectura Kafka | Esquema, topics por activo y prueba `verify 6` con partición/offset |
| S07 | Telemetría de mercado por WebSocket; reconciliación REST independiente | Velas cerradas, transporte/mode, heartbeat y continuidad por activo |
| S08 | Tres consultas: archivo, ventana fija y deslizante | Watermark 2 min; ventanas 5 min / paso 1 min; checkpoints separados |
| S09 | Métricas y reglas operativas; costo configurable | 21 familias propias, tres targets y ocho reglas; escenario USD 65,15/mes |
| S10 | Modelos persistidos aplicados a nuevos cierres | Siete series, versión/ancla/horizonte y verdad futura al madurar |
| S11 | KPIs y predicciones en un único Grafana | 23 paneles de datos, 14 filas de evaluación e historial de cruces |
| S12 | Integración, recuperación e idempotencia | Evidencia versionada, duplicado contado una vez, watermark y reinicio |

### S06 · Eventos y contrato

`market.klines.btcusdt` y `market.klines.solusdt` reciben objetos con identidad `symbol:1m:open_ms`, OHLC, actividad, cierre, origen y versión. Se publican con clave por símbolo, idempotencia Kafka y confirmación antes de avanzar el cursor REST. Cada topic tiene tres particiones y replicación 1.

El archivo valida rangos, tipos, OHLC y tiempo. Los rechazos Kafka se conservan en PostgreSQL; `market.deadletter` existe, pero no recibe automáticamente esos rechazos. Las claves SQL evitan duplicar velas/predicciones bajo reintentos. [Contrato e ingesta en detalle](pipeline.md).

### S07 · Fuente de telemetría del brief

La conexión pública combinada recibe `btcusdt@kline_1m` y `solusdt@kline_1m`. Solo `k.x=true` entra al análisis. REST consulta `/api/v3/time` y `/api/v3/klines`, revisita tres horas y recupera minutos intermedios aunque ya haya llegado un cierre posterior. No necesita claves privadas Binance.

!!! note "Adaptación del instrumento"
    El brief utiliza telemetría real de mercado como fuente continua. No se conectó un sensor físico ni se añadió un segundo caso empresarial/IoT ficticio. La plantilla del curso incluye ambos tipos de ingesta; la equivalencia del instrumento propio debe ser valorada por el docente. Esta documentación no declara ese criterio automáticamente acreditado.

### S08 · Ventanas, datos tardíos y recuperación

El archivo no usa watermark para descartar reparaciones antiguas. Las consultas de ventanas sí aplican watermark de dos minutos y deduplicación acotada por evento. Sus resultados incluyen cantidad de velas, volumen USDT y media de cierres; una ventana finalizada puede tener cobertura incompleta. Una reparación tardía del archivo no reabre una ventana ya purgada.

Los checkpoints persisten offsets y estado. La garantía es entrega al menos una vez con efectos idempotentes por clave, sin transacción global Kafka–PostgreSQL. [Proceso y diccionario SQL](pipeline.md).

### S09 · Observabilidad y criterio de escalado

Prometheus distingue transporte, heartbeat, edad de velas, huecos, edad de pronósticos y lag. `product_ready` exige los dos activos con 61 velas consecutivas y siete series recientes. Conexión WebSocket y 7/7 por separado son controles parciales.

El costo ilustrativo usa 730 h, 3,5 cores, 5,5 GiB RAM y 20 GiB disco: `730 × (3,5 × 0,02 + 5,5 × 0,003) + 20 × 0,1 = 65,145 USD/mes`. No es un cobro Docker ni una tarifa contratada.

Con dos activos hay normalmente dos cierres nuevos por minuto, más recuperaciones. Antes de escalar se observarían duración de lotes frente al trigger de 15 s, lag sostenido, memoria del driver y acumulación de pendientes. Más activos pueden requerir particionado por clave, sinks distribuidos, réplicas y redimensionar PostgreSQL; no existe un benchmark que garantice un límite de carga. [Métricas y ocho reglas explicadas](observabilidad.md).

### S10 · Inferencia y evaluación temporal

`ModelBundle` carga el registro una vez, reutiliza `past_features` y aplica pipelines. Las etiquetas futuras no se usan como entradas. Los horizontes son 15/30/5/15 minutos; `actual` se incorpora solo con las velas consecutivas completas y las reglas de la versión original.

El test histórico reservado muestra RMSE 0,12919 frente a 0,15080 en volatilidad; Brier 0,02500 frente a 0,02622 en caída; Brier 0,03913 frente a 0,06584 en actividad. Divergencia conserva cero. La precisión de caída es 13,54 % y recall 26,21 %: la mejora probabilística no basta para operar sin revisión. [Variables, muestras y métricas](modelos.md).

### S11 · BI y ML en el mismo tablero

Grafana consulta PostgreSQL para mercado/modelos y Prometheus para operación. El centro muestra valor, umbral, unidad, antigüedad, explicación y acción. NORMAL es evaluación válida sin cruce; REFERENCIA identifica baseline; datos ausentes o antiguos quedan explícitos. El historial no crea alarmas para llenar espacio. [Lectura de cada panel](dashboard.md).

### S12 · Integración demostrable

La aceptación guardada del **28 de octubre de 2026, 20:25 Lima**, usa la versión `20261029T011759858407Z.cal`: 40 pruebas de regresión y cinco Spark PASS, integración de las cuatro dimensiones y una interrupción real de 75 s detectada/recuperada. Las siete series avanzaron tres minutos entre muestras; se comprobaron 16 consultas SQL y siete expresiones Prometheus.

Son resultados fechados. Las [evidencias](evidencias.md) diferencian aceptación histórica, nuevas comprobaciones y capturas. Para ejecutarlo de nuevo, seguir [reproducibilidad](ejecucion.md); no copiar un PASS anterior para simular una ejecución.

## 2. Correspondencia con la rúbrica de evaluación

Esta matriz conserva los ocho ámbitos y ponderaciones de la [plantilla oficial U2](https://262bigdata.github.io/lambda26/proyecto-sello/u2-producto/), y localiza evidencia de CryptoPulse. La calificación y la sustentación individual corresponden al docente.

| Ámbito | Peso | Evidencia y límite |
|---|---:|---|
| Eventos con Kafka | 12 % | Topics/contrato y prueba de publicación–lectura sobre el instrumento de mercado |
| IoT o sensores | 10 % | Adaptación a telemetría de mercado; no hardware físico, equivalencia pendiente de evaluación docente |
| Procesamiento con estado | 14 % | Ventanas, watermark y checkpoint; ensayo de reinicio/duplicados |
| Observabilidad | 12 % | Métricas, reglas y paneles; no hay envío externo de alertas configurado |
| Costos y escalado | 8 % | Fórmula y supuestos explícitos frente a dos cierres por minuto |
| Salida BI/ML | 12 % | Grafana unificado con KPIs y predicciones |
| Inferencia temporal validada | 12 % | Partición cronológica, baseline y evaluación posterior |
| Sustentación | 20 % | Aporte individual, comunicación, presentación, repositorio, documentación y demo deben comprobarse durante la exposición |

No se asignan notas por tener una captura. El código y las pruebas respaldan capacidades; la presentación individual no puede certificarse mediante este sitio.

## 3. Trazabilidad y procedencia

El dominio proviene del brief CryptoPulse; la estructura pedagógica, de la plantilla U2. La implementación se encuentra en `pyspark/sesiones/cryptopulse/`, `platform.yml`, `database/`, `observability/` y `scripts/`. [Código fuente](https://github.com/abdul8377/big-data-btc-sol), [bitácora S01–S12](sesiones.md) e [informe integral](https://raw.githubusercontent.com/abdul8377/big-data-btc-sol/main/site/resources/informe-integral-cryptopulse.md) completan la trazabilidad.

La documentación publicada es estática; los servicios siguen siendo locales. La versión del modelo y las fechas acompañan las evidencias para no presentar resultados guardados como estado actual del mercado.
