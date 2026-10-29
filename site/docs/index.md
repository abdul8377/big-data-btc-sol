<section class="hero" markdown="1">
<span class="eyebrow">Big Data · Arquitectura Lambda · BTCUSDT + SOLUSDT</span>
# CryptoPulse
**Del dato histórico al riesgo de mercado en tiempo real.**

Plataforma académica que integra ingesta verificable, procesamiento distribuido, modelos MLlib, streaming con estado y visualización en Grafana. Examina la volatilidad, las caídas significativas, la actividad anómala y la divergencia entre Bitcoin y Solana.

<div class="hero-links">
<a class="cp-button" href="brief/">Explorar el proyecto</a>
<a class="cp-button secondary" href="u2-producto/">Ver entregable U2</a>
<a class="cp-button secondary" href="dashboard/">Explorar los paneles</a>
</div>
</section>

<div class="kpi-grid">
<div class="kpi"><span class="kpi-number">4,99 M</span><span class="kpi-label">Velas históricas de entrada</span></div>
<div class="kpi"><span class="kpi-number">114</span><span class="kpi-label">Archivos mensuales verificados</span></div>
<div class="kpi"><span class="kpi-number">4</span><span class="kpi-label">Dimensiones analíticas</span></div>
<div class="kpi"><span class="kpi-number">S01–S12</span><span class="kpi-label">Capacidades documentadas</span></div>
</div>

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · HOME-01</span>
<h4>Portada del dashboard CryptoPulse</h4>
<p>Captura panorámica de Grafana mostrando título, estado integral, BTC y SOL y navegación del tablero.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/home-01.png</code></span>
</div>

## ¿Qué problema aborda?

Una serie aislada de precios no explica por sí misma la intensidad del movimiento, la probabilidad de una caída significativa, la actividad atípica ni si BTC y SOL se desacoplan. CryptoPulse combina cuatro perspectivas y separa el comportamiento observado de las estimaciones futuras, indicando estados y calidad de los datos.

<div class="dimension-grid">
<div class="dimension-card"><span class="cp-tag">E2 · 15 min</span><h3>Volatilidad</h3><p>Intensidad de variaciones observadas y estimación futura mediante Random Forest.</p></div>
<div class="dimension-card"><span class="cp-tag">E5 · 30 min</span><h3>Caídas significativas</h3><p>Estimación probabilística de un evento bajo el umbral histórico por activo.</p></div>
<div class="dimension-card"><span class="cp-tag">E6 · 5 min</span><h3>Actividad anómala</h3><p>Volumen y operaciones frente a referencias por activo y hora UTC.</p></div>
<div class="dimension-card"><span class="cp-tag">E4 · 15 min</span><h3>Correlación y divergencia</h3><p>Relación entre rendimientos y diferencia porcentual de SOL respecto a BTC.</p></div>
</div>

## El recorrido del dato

**Batch:** archivos mensuales oficiales → SHA256 → validación y cuarentena → Parquet → variables → entrenamiento y evaluación temporal.

**Streaming:** Binance WebSocket con recuperación REST → Kafka → Spark Structured Streaming → PostgreSQL → Grafana. Prometheus observa la salud operativa.

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · HOME-02</span>
<h4>Mapa visual de la arquitectura</h4>
<p>Diagrama completo del flujo batch/streaming y capa de consulta. Puede generarse a partir de docs/arquitectura.mmd del repositorio.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/home-02.png</code></span>
</div>

<div class="cp-callout" markdown="1">**Alcance:** el sistema documentado funciona en un entorno local. No ejecuta operaciones financieras ni garantiza pronósticos. Las métricas citadas describen las ejecuciones documentadas, no el mercado en este instante.</div>

**Continuar:** [Brief y preguntas](brief.md) · [Arquitectura](arquitectura.md) · [Unidad II](u2-producto.md) · [Código fuente](https://github.com/abdul8377/big-data-btc-sol)
