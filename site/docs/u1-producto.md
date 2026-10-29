# Producto · Unidad I

<span class="cp-tag">S01–S05 · Capa batch</span>

## Producto demostrable

La Unidad I construye una base histórica confiable para dos criptomonedas y modelos reproducibles con PySpark, desde obtención y validación hasta entrenamiento temporal y verificación integrada.

| Sesión | Capacidad | Evidencia o archivo |
|---|---|---|
| S01 | Arquitectura, Docker, Jupyter y Spark | [Notebook S01](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/s01-arquitectura/01_arquitectura_practica.ipynb) |
| S02 | DataFrame, RDD, agregados, lazy evaluation | [Notebook S02](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/s02-fundamentos/02_fundamentos_practica.ipynb) |
| S03 | Calidad, cuarentena, huecos, Parquet | [quality.py](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/cryptopulse/quality.py) |
| S04 | Entrenamiento MLlib y validación temporal | [training.py](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/sesiones/cryptopulse/training.py) |
| S05 | Integración batch | [verificar_producto.ps1](https://github.com/abdul8377/big-data-btc-sol/blob/main/scripts/verificar_producto.ps1) |

## Procedencia y calidad

De 4 993 759 filas históricas iniciales, se conservaron **4 993 757 válidas**, con **dos en cuarentena** y **tres huecos** que suman 163 minutos por activo. Se documentaron estas ausencias, sin interpolar precios.

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U1-01</span>
<h4>S01 · Entorno en ejecución</h4>
<p>Docker, Jupyter y Spark UI con la SparkSession activa.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u1-01.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U1-02</span>
<h4>S01 · Lectura de velas reales</h4>
<p>Notebook con 2 880 registros del muestreo de enero de 2025 y esquema de BTC/SOL.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u1-02.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U1-03</span>
<h4>S02 · Transformaciones y lazy evaluation</h4>
<p>Notebook con filter/select/groupBy, plan de ejecución y comparación antes/después de una acción.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u1-03.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U1-04</span>
<h4>S02 · Agregados y Parquet</h4>
<p>Tabla de agregados OHLC/VWAP y vista de lectura/escritura Parquet.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u1-04.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U1-05</span>
<h4>S03 · Calidad y cuarentena</h4>
<p>Conteos de entrada, válidos, rechazados, huecos y razones de exclusión.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u1-05.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U1-06</span>
<h4>S03 · Partition pruning</h4>
<p>Plan físico Spark mostrando PartitionFilters y carpeta de salida particionada.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u1-06.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U1-07</span>
<h4>S04 · Modelos MLlib</h4>
<p>Entrenamiento de cuatro dimensiones, métricas y selección frente a baselines.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u1-07.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U1-08</span>
<h4>S05 · Verificación del batch</h4>
<p>Consola de verificación de Unidad I y artefactos generados.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u1-08.png</code></span>
</div>

## Trazabilidad y entregables

Cada fila de origen tiene procedencia registrada, cada transformación se ubica en un archivo versionado y las predicciones se contrastan con una partición de prueba posterior. No se asignan puntajes ni acreditación docente hasta incorporar la rúbrica oficial y la evidencia visual de la ejecución.

**Referencia:** [Informe por sesiones](https://github.com/abdul8377/big-data-btc-sol/blob/main/docs/INFORME_SESIONES_S01_S12.md).
