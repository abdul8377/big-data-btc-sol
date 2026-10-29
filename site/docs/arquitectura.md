# Arquitectura del sistema

## Vista de alto nivel

CryptoPulse emplea una organización de tipo **Lambda**: una capa batch para históricos y entrenamiento, una capa de velocidad para eventos recientes y una capa de consulta que consolida resultados en PostgreSQL y Grafana. El diagrama versionado se encuentra en [docs/arquitectura.mmd](https://github.com/abdul8377/big-data-btc-sol/blob/main/docs/arquitectura.mmd).

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · ARQ-01</span>
<h4>Diagrama general Lambda</h4>
<p>Diagrama de Binance → histórico + WebSocket → Kafka/Spark → PostgreSQL → Grafana y de modelos batch reutilizados en inferencia.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/arq-01.png</code></span>
</div>

<div class="info-grid">
<div class="info-card"><h3>Batch · Unidad I</h3><p>ZIP/CSV con procedencia SHA256; validación y cuarentena; Parquet particionado; generación de variables; MLlib con separación cronológica y modelos versionados.</p></div>
<div class="info-card"><h3>Streaming · Unidad II</h3><p>Velas cerradas por WebSocket, reconciliación REST, Kafka con contrato, Structured Streaming con ventanas, watermark, deduplicación y checkpoint.</p></div>
<div class="info-card"><h3>Serving</h3><p>PostgreSQL contiene señales, agregados, predicciones y resultados; Grafana expone métricas de mercado y operación.</p></div>
<div class="info-card"><h3>Observabilidad</h3><p>Prometheus, exportador, reglas operativas y verificaciones de frescura, continuidad y salud del pipeline.</p></div>
</div>

## Flujo batch

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · ARQ-02</span>
<h4>Pipeline histórico</h4>
<p>Ejecución de calidad, lectura de archivos, tabla de cuarentena, Parquet por activo/año/mes y registro de modelos.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/arq-02.png</code></span>
</div>
Los archivos se contrastan con SHA256. La aplicación registra filas inválidas y huecos, en vez de fabricar velas. Las variables se derivan únicamente de información disponible al cierre de cada observación.

## Flujo streaming

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · ARQ-03</span>
<h4>Pipeline en tiempo real</h4>
<p>Kafka UI mostrando mensajes del productor de BTC y SOL; Spark streaming consumiendo eventos y escribiendo a PostgreSQL.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/arq-03.png</code></span>
</div>
El productor evita publicar velas todavía abiertas y dispone de recuperación REST. Spark crea ventanas fijas y deslizantes de cinco minutos, deduplica identificadores y utiliza watermark de dos minutos. Los checkpoints permiten reiniciar el procesamiento. Las claves únicas de PostgreSQL evitan duplicados persistidos: no debe confundirse con una garantía de transacción global exactamente una vez.

## Despliegue y dependencias

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · ARQ-04</span>
<h4>Contenedores y servicios</h4>
<p>Captura de Docker Desktop o docker compose ps mostrando los contenedores activos de la plataforma.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/arq-04.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · ARQ-05</span>
<h4>Persistencia y tablas</h4>
<p>Captura del esquema SQL en PostgreSQL, tablas principales y ejemplos de consultas de resultados.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/arq-05.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · ARQ-06</span>
<h4>Observabilidad</h4>
<p>Captura de métricas de Prometheus y reglas relacionadas con salud del pipeline.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/arq-06.png</code></span>
</div>

**Código:** [platform.yml](https://github.com/abdul8377/big-data-btc-sol/blob/main/platform.yml) · [stream.py](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/cryptopulse/stream.py) · [database/init.sql](https://github.com/abdul8377/big-data-btc-sol/blob/main/database/init.sql).
