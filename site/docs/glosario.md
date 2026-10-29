# Glosario del proyecto

## Glosario aplicado

**Activo base:** BTC o SOL, unidad del volumen base. **Cotización:** USDT, unidad monetaria del precio/importe. **Spot:** compra/venta del activo en ese mercado, distinto de futuros. **Vela/OHLC:** resumen de apertura, máximo, mínimo y cierre de un intervalo.

**Batch:** procesamiento finito de un conjunto histórico. **Streaming:** procesamiento sucesivo de entradas nuevas. **Micro-lote:** grupo de entradas procesadas en una ejecución del trigger. **Event time:** tiempo del hecho de mercado. **Processing time:** tiempo del motor que lo procesa. **Ancla/as of:** último cierre cuya información se permite usar en una predicción.

**Feature:** variable de entrada calculada hasta el ancla. **Etiqueta/target:** resultado futuro usado para aprender/evaluar. **Horizonte:** cuánto después del ancla se define ese resultado. **Fuga temporal:** utilizar información futura en una entrada o decisión que se supone tomada antes. **Embargo/purga:** margen de exclusión para impedir que una etiqueta atraviese una frontera de evaluación.

**Baseline:** referencia sencilla que un candidato debe mejorar. **Calibración:** ajuste para acercar probabilidades estimadas a frecuencias observadas. **Percentil P5/P95:** valor bajo el cual cae aproximadamente 5 %/95 % de una muestra definida; no una garantía sobre todos los períodos futuros. **Prevalencia:** proporción de positivos en esa muestra.

**Topic:** nombre de un log Kafka. **Partición:** tramo ordenado e independiente de un topic. **Offset:** posición dentro de una partición. **Lag:** distancia de posiciones entre disponibilidad y procesamiento. **Checkpoint:** estado durable para continuar una consulta. **Watermark:** criterio de avance temporal/tardanza del motor. **Idempotencia:** repetir una operación con la misma identidad conserva un único efecto por su clave.

**KPI:** indicador resumido de datos/resultado. **Métrica operativa:** medición técnica del funcionamiento. **Señal de mercado:** evaluación de una condición financiera definida. **Alerta técnica:** regla sobre la salud del proceso. **Frescura:** antigüedad del dato relevante. **Continuidad:** existencia de los minutos requeridos, incluso si el dato más reciente está al día.
