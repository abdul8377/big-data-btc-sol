# Dimensiones, variables y modelos

La interpretación distingue claramente **medición observada**, **predicción** y **referencia**. Una señal no implica automáticamente una recomendación financiera.

| Dimensión | Horizonte | Método seleccionado | Unidad del resultado |
|---|---:|---|---|
| Volatilidad futura | 15 min | Random Forest | porcentaje no anualizado |
| Caída significativa | 30 min | Regresión logística calibrada | probabilidad |
| Actividad anómala | 5 min | Regresión logística calibrada | probabilidad |
| Divergencia SOL–BTC | 15 min | Referencia constante de cero | puntos porcentuales |

El candidato de regresión para divergencia no mejoró la referencia simple en validación; por eso el tablero muestra el baseline y no afirma una ventaja predictiva inexistente.

## Preparación del aprendizaje

Se generan rendimientos, variabilidad, indicadores de actividad, rango de vela y relaciones entre activos. La partición temporal documentada usa **2022–2024 entrenamiento, 2025 validación, enero–septiembre 2026 prueba**, con embargo de 30 minutos en los límites pertinentes. Las etiquetas futuras no forman parte de las variables de entrada.

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · MOD-01</span>
<h4>Construcción de variables</h4>
<p>Notebook o salida de Spark mostrando rendimientos y variables derivadas con esquema visible.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/mod-01.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · MOD-02</span>
<h4>Particiones temporales</h4>
<p>Gráfico de línea de tiempo entrenamiento, validación/calibración, selección y prueba, con embargo.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/mod-02.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · MOD-03</span>
<h4>Evaluación comparativa de modelos</h4>
<p>Captura de tabla de métricas por dimensión y por activo, contrastando modelos con baseline.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/mod-03.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · MOD-04</span>
<h4>Registro de modelos versionados</h4>
<p>Captura de modelos, umbrales y versiones almacenados tras el entrenamiento.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/mod-04.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · MOD-05</span>
<h4>Inferencia en streaming</h4>
<p>Captura de predicciones emitidas por cierre de vela con timestamp y horizonte.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/mod-05.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · MOD-06</span>
<h4>Evaluación diferida</h4>
<p>Captura con predicción original y valor real después de madurar el horizonte, evitando fuga temporal.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/mod-06.png</code></span>
</div>

## Lectura responsable de los resultados

La precisión en eventos raros puede ser limitada. En el informe de revisión se documentó que el clasificador de caídas tiene falsos positivos y eventos omitidos; presenta sus métricas junto con su cobertura, no como certeza. Una correlación elevada no demuestra causalidad.

**Implementación:** [features.py](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/cryptopulse/features.py) · [training.py](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/cryptopulse/training.py) · [inference.py](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/cryptopulse/inference.py).
