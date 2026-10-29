# Producto · Unidad III / presentación final

<span class="cp-tag">Cierre · Demostración · Reproducibilidad</span>

Esta sección agrupa la **demostración final del producto**. No presupone que se hayan completado competencias o evaluaciones de una tercera unidad no verificadas en el repositorio.

## Caso de demostración

1. Iniciar Docker y servicios con el script de arranque.
2. Verificar la ingesta de velas reales de ambos activos.
3. Mostrar agregados históricos y salidas del entrenamiento.
4. Demostrar predicciones nuevas, su maduración y su comparación con realidad.
5. Mostrar Grafana, salud del pipeline y estados de las señales.
6. Ejecutar pruebas integrales y explicar límites.

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U3-01</span>
<h4>Demo de arranque</h4>
<p>Terminal con docker compose ps y resumen de servicios listos.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u3-01.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U3-02</span>
<h4>Demo end-to-end</h4>
<p>Secuencia del evento de Binance a Kafka, Spark, PostgreSQL y Grafana con timestamps.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u3-02.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U3-03</span>
<h4>Resultados analíticos</h4>
<p>Panel conjunto que explique claramente cuatro dimensiones, sus umbrales y las diferencias BTC–SOL.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u3-03.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U3-04</span>
<h4>Salud y recuperación</h4>
<p>Captura de prueba de caída y restauración controlada sin fabricar registros faltantes.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u3-04.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · U3-05</span>
<h4>Limitaciones y mejora</h4>
<p>Lámina que documente errores de clasificación, baseline de divergencia y plan de mejora futuro.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/u3-05.png</code></span>
</div>

## Resultado documentado

El informe final registra **40 pruebas de regresión** y **5 pruebas con Spark** aprobadas, así como una verificación de integración y una interrupción controlada de 75 segundos. Los estados exactos son de aquella ejecución y deben revisarse otra vez antes de exponerlos como estado actual.

## Entregables verificables

Código, guías de ejecución, fuentes de datos, definición de Grafana, esquema de contratos, modelos versionados y capturas que demuestren los resultados. La evaluación formal corresponde al docente y a su rúbrica vigente.

[Producto final versionado](https://github.com/abdul8377/big-data-btc-sol/blob/main/docs/PRODUCTO_FINAL.md).
