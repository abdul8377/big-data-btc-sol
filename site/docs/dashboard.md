# Grafana y resultados

El dashboard local principal es `http://localhost:43000/d/cryptopulse-main`. **No es una URL pública**: las capturas se incorporan aquí después de tomarlas en el equipo donde está ejecutándose la plataforma.

## Lectura recomendada

El tablero distingue estado integral, continuidad de datos, señales actuales, tendencias, evidencia de modelos y operación. El resultado documentado tiene siete series de predicción: volatilidad, caída y actividad por cada activo, más divergencia conjunta. No se fabrican alertas para hacer aparecer filas.

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · GRAF-01</span>
<h4>Vista panorámica</h4>
<p>Captura del dashboard completo, mostrando secciones y distribución general.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/graf-01.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · GRAF-02</span>
<h4>Salud integral 7/7</h4>
<p>Tarjeta de salud y continuidad de series, con timestamp.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/graf-02.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · GRAF-03</span>
<h4>BTC y SOL en el tiempo</h4>
<p>Paneles de precios y datos recientes expresados en USDT.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/graf-03.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · GRAF-04</span>
<h4>Centro de señales</h4>
<p>Estados, valores, umbrales, fechas y motivo de cada evaluación.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/graf-04.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · GRAF-05</span>
<h4>Historial de alertas reales</h4>
<p>Historial de condiciones que efectivamente cruzaron umbrales o mensaje explicativo de ausencia de cruces.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/graf-05.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · GRAF-06</span>
<h4>Volatilidad</h4>
<p>Gráfica observada y estimada, umbral histórico y unidades porcentuales.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/graf-06.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · GRAF-07</span>
<h4>Caídas significativas</h4>
<p>Probabilidades, eventos observados y referencias, sin interpretar la probabilidad como porcentaje de caída.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/graf-07.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · GRAF-08</span>
<h4>Actividad inusual</h4>
<p>Comparación de volumen y operaciones con sus referencias P95 por activo y hora.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/graf-08.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · GRAF-09</span>
<h4>Correlación y divergencia</h4>
<p>Gráficos de correlación móvil y rendimiento SOL menos BTC, con baseline seleccionado.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/graf-09.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · GRAF-10</span>
<h4>Rendimiento de modelos</h4>
<p>Panel con precisión, recall, errores, baseline y evaluación posterior al horizonte.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/graf-10.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · GRAF-11</span>
<h4>Procesamiento y costos</h4>
<p>Métricas Kafka/Spark, latencias, salud y costos estimados editables.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/graf-11.png</code></span>
</div>

!!! warning "Interpretación"
    Una fila NORMAL no significa riesgo nulo. Una señal predictiva no demuestra que ocurrirá el evento. Los datos faltantes o atrasados deben aparecer como tales.

**Configuración reproducible:** [cryptopulse.json](https://github.com/abdul8377/big-data-btc-sol/blob/main/observability/grafana/dashboards/cryptopulse.json).
