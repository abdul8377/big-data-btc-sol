# Brief técnico-analítico

<span class="cp-tag">CryptoPulse · BTCUSDT y SOLUSDT</span>

## Contexto y necesidad

Los mercados de criptomonedas producen datos continuos de precio, volumen y operaciones. Comparar únicamente cotizaciones dificulta distinguir movimientos habituales de escenarios que merecen seguimiento. CryptoPulse aplica ingeniería de datos y aprendizaje automático a un caso reproducible de mercado, no a una simulación de sensores.

## Pregunta central

**¿Cómo analizar y monitorear el comportamiento de BTC y SOL integrando datos históricos y eventos en tiempo real para identificar condiciones relevantes de riesgo y divergencia mediante una arquitectura Big Data?**

## Preguntas por dimensión

| Dimensión | Pregunta batch (histórica) | Pregunta streaming (en vivo) |
|---|---|---|
| Volatilidad | ¿Cómo varía la volatilidad histórica por activo y período, y qué aporta un modelo frente a la referencia? | ¿Cuándo la volatilidad reciente o prevista rebasa su referencia histórica? |
| Caídas | ¿Con qué frecuencia aparecen caídas bajo el percentil histórico y cómo se comporta su clasificación? | ¿Cuál es la probabilidad estimada de caída en el próximo horizonte de 30 minutos? |
| Anomalías | ¿Qué umbrales de volumen y operaciones son razonables por activo y hora UTC? | ¿Cuándo el mercado observado o previsto presenta actividad superior a la referencia? |
| Correlación y divergencia | ¿Cómo cambian la correlación y la diferencia de rendimientos entre ambos activos? | ¿Se observa una divergencia significativa BTC–SOL en los datos recientes? |

Las preguntas son formulaciones documentales coherentes con la implementación; no sustituyen el brief originalmente entregado por el equipo.

## Alcance técnico

- **Activos:** BTCUSDT y SOLUSDT, Binance Spot público.
- **Granularidad:** velas cerradas de un minuto.
- **Histórico:** enero 2022–septiembre 2026; 114 ZIP mensuales, con verificación SHA256.
- **Tareas:** calidad, agregados, entrenamiento de modelos, inferencia, estado de alertas y visualización.
- **Límites:** sin ejecución de órdenes, sin datos de cuentas de usuarios, sin promesa de rentabilidad.

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · BRIEF-01</span>
<h4>Problema y propuesta de valor</h4>
<p>Esquema o lámina que contraste análisis aislado de cotizaciones frente a la visión integrada de CryptoPulse.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/brief-01.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · BRIEF-02</span>
<h4>Fuentes de datos</h4>
<p>Captura de Binance Public Data mostrando los archivos de BTCUSDT y SOLUSDT y la periodicidad de un minuto.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/brief-02.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · BRIEF-03</span>
<h4>Plan del proyecto</h4>
<p>Cronograma o tablero de implementación de las sesiones S01 a S12 y entregables por unidad.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/brief-03.png</code></span>
</div>

## Criterios de éxito

Trazabilidad al origen, reproducibilidad de transformaciones, evaluación cronológica sin fuga de información, separación entre riesgo observado y predicho, ingesta recuperable e identificación explícita de datos ausentes o desactualizados.

**Fuentes internas:** [README](https://github.com/abdul8377/big-data-btc-sol/blob/main/README.md), [Producto final](https://github.com/abdul8377/big-data-btc-sol/blob/main/docs/PRODUCTO_FINAL.md).
