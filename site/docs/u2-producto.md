# Producto · Unidad II

<span class="cp-tag">S06–S12 · Kafka · Streaming · Observabilidad · Serving</span>

## 1. Resultado integrado

La Unidad II añade una capa de velocidad a CryptoPulse: captura de velas cerradas por activo, distribución a Kafka, procesamiento con estado en Spark, inferencia con modelos del batch y publicación de resultados en PostgreSQL y Grafana.

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-01</span>
<h4>Producto final · Pantalla principal</h4>
<p>Captura de Grafana que muestre estado 7/7, información actual de BTC y SOL y organización del tablero.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-01.png</code></span>
</div>

## 2. Capacidades y correspondencia con las sesiones

| Sesión | Competencia aplicada a CryptoPulse | Soporte versionado |
|---|---|---|
| S06 | Contrato de eventos y publicación/consumo Kafka | [contract.py](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/cryptopulse/contract.py), [schema](https://github.com/abdul8377/big-data-btc-sol/blob/main/schemas/market-kline-v1.schema.json) |
| S07 | Fuente de telemetría continua: mercado Binance | [producer.py](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/cryptopulse/producer.py) |
| S08 | Ventanas, watermark, deduplicación, checkpoint | [stream.py](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/cryptopulse/stream.py) |
| S09 | Observabilidad, alertas y costos orientativos | [exporter.py](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/cryptopulse/exporter.py), [alerts.yml](https://github.com/abdul8377/big-data-btc-sol/blob/main/observability/alerts.yml) |
| S10 | Inferencia y evaluación diferida | [inference.py](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/cryptopulse/inference.py) |
| S11 | KPIs y paneles de decisión | [Dashboard JSON](https://github.com/abdul8377/big-data-btc-sol/blob/main/observability/grafana/dashboards/cryptopulse.json) |
| S12 | Integración, recuperación, validaciones | [integration.py](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/cryptopulse/integration.py) |

!!! note "Adaptación del caso de telemetría"
    S07 utiliza eventos reales del mercado como flujo análogo de telemetría. No se presenta como la conexión de un ESP32 o un sensor IoT físico. La equivalencia académica debe confirmarse con el docente si el criterio exige hardware.

### S06 · Kafka y contrato

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-02</span>
<h4>Topics Kafka</h4>
<p>Kafka UI mostrando topics de BTC y SOL, particiones y mensajes.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-02.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-03</span>
<h4>Evento bajo contrato</h4>
<p>Payload de vela cerrada con símbolo, temporalidad, timestamps, valores OHLC y versión del esquema.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-03.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-04</span>
<h4>Publicación y consumo</h4>
<p>Evidencia de productor y consumidor con recepción de mensajes reales en Kafka.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-04.png</code></span>
</div>

### S07 · Ingesta continua

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-05</span>
<h4>Conexión Binance WebSocket</h4>
<p>Registro del productor suscrito a BTCUSDT y SOLUSDT kline_1m y recepción de cierres.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-05.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-06</span>
<h4>Reconciliación REST</h4>
<p>Log o prueba que demuestre la recuperación de una vela faltante y control contra duplicados.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-06.png</code></span>
</div>

### S08 · Streaming con estado

Se utilizan ventanas de cinco minutos (fijas y deslizantes con paso de un minuto), watermark de dos minutos, identificadores estables y checkpoints. Los eventos demasiado tardíos pueden conservarse en archivo durable aunque no alimenten una ventana cerrada.

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-07</span>
<h4>Consulta Structured Streaming</h4>
<p>Consola de Spark mostrando queries activas, batchId, filas procesadas y salida a PostgreSQL.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-07.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-08</span>
<h4>Ventanas y watermark</h4>
<p>Tabla o salida con inicio/fin de ventana, activos, agregados y manejo de datos tardíos.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-08.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-09</span>
<h4>Deduplicación y checkpoint</h4>
<p>Prueba con mensaje repetido y recuperación después de reiniciar, sin duplicación en PostgreSQL.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-09.png</code></span>
</div>

### S09 · Observabilidad y FinOps

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-10</span>
<h4>Prometheus /targets</h4>
<p>Vista de targets y recopilación de métricas.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-10.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-11</span>
<h4>Alertas operativas</h4>
<p>Reglas y estados de frescura, retraso Kafka y actividad del productor/streaming.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-11.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-12</span>
<h4>Estimación de costo</h4>
<p>Salida del escenario de costos CPU, RAM y disco, marcada como estimación editable, no facturación.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-12.png</code></span>
</div>

### S10 · Inferencia online

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-13</span>
<h4>Predicciones por activo</h4>
<p>Resultado de inferencia de volatilidad, caída y actividad para BTC y SOL, más divergencia conjunta.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-13.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-14</span>
<h4>Evaluación del horizonte madurado</h4>
<p>Predicciones emitidas y valores posteriores reales, con timestamps y versión del modelo.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-14.png</code></span>
</div>

### S11 · Integración de resultados

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-15</span>
<h4>Grafana · Señales actuales</h4>
<p>Panel con estados NORMAL, VIGILANCIA, ALERTA, REFERENCIA o datos desactualizados.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-15.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-16</span>
<h4>Grafana · Riesgo por dimensión</h4>
<p>Gráficas de volatilidad, caída, anomalías y divergencia, comparando observado y previsto.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-16.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-17</span>
<h4>Grafana · Estado del pipeline</h4>
<p>Panel de salud, latencia, actividad Kafka y frescura de las series.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-17.png</code></span>
</div>

### S12 · Verificación extremo a extremo

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-18</span>
<h4>Ejecución de la validación S12</h4>
<p>Consola de verificación de la integración con PASS y fecha claramente visible.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-18.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-19</span>
<h4>Prueba de recuperación</h4>
<p>Evidencia de interrupción de productor, detección de degradación y recuperación sin huecos.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-19.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U2-20</span>
<h4>Validación SQL de paneles</h4>
<p>Consultas SQL de paneles y su resultado, confirmando que el tablero se alimenta de tablas reales.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u2-20.png</code></span>
</div>

## 3. Trazabilidad y procedencia de la rúbrica

Esta matriz sirve para **localizar y comprobar evidencias**, sin simular una calificación del docente.

| Componente a verificar | Qué debe demostrar la evidencia | Fuentes y ubicación |
|---|---|---|
| Eventos | Esquema versionado y evento realmente consumido | S06 · U2-02 a U2-04 |
| Ingesta | Fuente auténtica, continuidad y recuperación | S07 · U2-05 a U2-06 |
| Estado | Ventanas, watermark, duplicados y checkpoint | S08 · U2-07 a U2-09 |
| Operación | Métricas y diagnóstico de fallos | S09 · U2-10 a U2-12 |
| Modelos online | Inferencia sin reentrenar por mensaje y evaluación temporal | S10 · U2-13 a U2-14 |
| Visualización | KPIs, señales y fuentes interpretables | S11 · U2-15 a U2-17 |
| Producto integrador | Recuperación y pruebas extremo a extremo | S12 · U2-18 a U2-20 |

<div class="cp-callout warning" markdown="1">**Nota metodológica:** esta tabla es una **matriz de trazabilidad adaptada al proyecto**, no una reproducción literal ni una autocertificación de la rúbrica del curso. Para calificar, contrastar los criterios y ponderaciones oficiales en la [guía U2 del docente](https://262bigdata.github.io/lambda26/proyecto-sello/u2-producto/).</div>

## 4. Criterios de aceptación

Un dashboard visualmente cargado no constituye por sí solo una prueba. Para aceptar el producto debe verificarse que se actualizan **las siete series predictivas**, que existen datos observados recientes, que los huecos y duplicados se gestionan explícitamente y que las tablas de Grafana consultan resultados reales del pipeline. En la revisión final documentada se comprobaron pruebas de regresión, Spark e integración; esos resultados deben acompañarse de sus capturas.

## 5. Repositorio y reproducibilidad

[Ver informe de sesiones S01–S12](https://github.com/abdul8377/big-data-btc-sol/blob/main/docs/INFORME_SESIONES_S01_S12.md) · [Guía final](https://github.com/abdul8377/big-data-btc-sol/blob/main/docs/PRODUCTO_FINAL.md) · [Instrucciones de ejecución](ejecucion.md).
