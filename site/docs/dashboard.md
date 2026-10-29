# Grafana: cómo interpretar cada panel

## Grafana: explicación de cada elemento visible

### Fuentes, filtros y convenciones del tablero

La fuente PostgreSQL tiene UID `cryptopulse-postgres`; la de Prometheus, `cryptopulse-prometheus`. Grafana consulta SQL como `grafana_reader`. El dashboard UID `cryptopulse-main` tiene **28 elementos: 23 paneles de datos, una guía y cuatro filas de sección**. De los 23 paneles de datos, 16 consultan PostgreSQL y siete Prometheus.

El filtro temporal de predicciones/historial usa las anclas dentro del intervalo seleccionado. La verdad futura que aparece corresponde a esas mismas anclas cuando ya ha madurado. No todas las tablas usan el selector: precios, estado actual, actividad más reciente, histórico limpio y cobertura total de ventanas siguen sus consultas propias. Cada caso se identifica abajo.

Los gráficos no unen deliberadamente nulos (`spanNulls=false`). Un hueco visible puede representar datos o resultados faltantes; no debe suavizarse suponiendo valores intermedios. Los gráficos de probabilidad usan escala interna 0–1 y formato de porcentaje; la tabla de señales muestra el número original con unidad explícita. Volatilidad usa escala porcentual directa y divergencia puntos porcentuales.

Los paneles stat de precios y conteos usan azul neutral para evitar el umbral rojo genérico de Grafana a partir de 80. Un precio BTC mayor de 80 USDT no constituye una alerta. Solo el estado integral posee su mapeo propio rojo/verde.

### ID 30 — Estado integral del producto

Tipo stat, fuente Prometheus, expresión `cryptopulse_product_ready`. 1 se muestra LISTO en verde; 0, REVISAR DATOS / INFERENCIA en rojo. Debe ser el primer control antes de interpretar el mercado. Reúne heartbeats, continuidad, versión y siete pronósticos recientes. Si no llega la métrica, no se puede inferir que está listo a partir del último precio visible.

### ID 2 — BTCUSDT · cierre de 1 minuto

Tipo stat, PostgreSQL. Busca el último cierre de BTC en `market_candles` excluyendo replay. La unidad es USDT por BTC. La consulta devuelve `now()` para representar el stat, pero el valor económico pertenece al último cierre; **esa hora de consulta no actualiza el precio ni demuestra frescura**. La tabla por activo aporta su edad real. No depende del intervalo elegido para el gráfico.

### ID 3 — SOLUSDT · cierre de 1 minuto

Mismo proceso que BTC para SOL, en USDT por SOL. Permite seguir el cierre confirmado de ese mercado, no compararlo directamente con BTC por precio nominal. La comparación relativa se realiza con rendimientos y divergencia. También necesita leer la edad de su propia vela.

### ID 31 — Pronósticos vigentes · objetivo 7/7

Tipo stat, PostgreSQL. Cuenta filas forecast de `cp_signal_board` con edad de hasta 180 segundos. El objetivo es 7, incluyendo la referencia desplegada de divergencia. Comprueba frescura de filas existentes, no que todas sean modelos ML ganadores ni que las 61 velas estén completas. Usarlo junto al estado integral y la tabla por activo.

### ID 32 — Frescura y continuidad por activo

Tabla PostgreSQL sobre `cp_asset_health`. Muestra activo, estado, edad de vela en segundos, cantidad de velas de contexto de 61, minutos internos faltantes, edad de indicadores y último cierre.

SIN DATOS indica ausencia del archivo; DESACTUALIZADO, edad superior a tres minutos; CALENTANDO, menos de 61 registros; DATOS INCOMPLETOS, huecos; LISTO, cumplimiento de los requisitos de esa vista de mercado. La vista es parcial respecto de la salud integral. La edad KPI puede revelar que llegan precios pero no avanzan indicadores. La tabla siempre considera el estado reciente, no el período histórico elegido.

### ID 33 — Cómo utilizar CryptoPulse

Elemento de texto, sin consulta ni métrica. Explica el orden de uso: LISTO y 7/7, observaciones/pronósticos, contraste y seguimiento. Indica horizontes 5/15/30 y que la señal sirve para revisar. Es parte de la interfaz aunque no se cuente entre los 23 paneles de datos.

### ID 10 — Señales de mercado · estado, umbral y explicación

Tabla PostgreSQL de `cp_signal_board`. Cada fila identifica activo, dimensión y origen observada/pronóstico. Incluye estado, valor redondeado, umbral, unidad, edad, interpretación y qué revisar. Su lectura completa evita confundir 0,058 de probabilidad con una caída de 5,8 % o un ratio de actividad con USDT.

NORMAL significa evaluación disponible sin cruce; VIGILANCIA, cruce de una regla; ALERTA, cruce de caída; REFERENCIA, baseline predictivo; NO DISPONIBLE, variable/umbral inválido; SIN EVALUAR, ausencia de señal; DESACTUALIZADO, más de tres minutos. La antigüedad tiene prioridad para evitar que una alerta vieja se interprete como nueva.

El panel resuelve el problema anterior de quedar en No data cuando no había alarmas: muestra evaluaciones normales y explica ausencias. No depende del intervalo del dashboard, porque es el estado actual. Su objetivo de catorce filas requiere que exista la configuración de versión activa; si está vacía también hay un problema que revisar.

### ID 34 — Historial de cruces · período seleccionado

Tabla PostgreSQL sobre `market_signals`, solo cruces reales `triggered`, live y de la versión activa. Ordena por ancla descendente y limita a 100 filas. Incluye fecha, activo, dimensión, origen, severidad, valor, umbral, detalle, acción y versión.

Si no hay eventos, genera una fila explicativa SIN CRUCES cuando detecta siete pronósticos recientes, o REVISAR INFERENCIA si no los detecta. Ese fallback usa el contador de frescura; no sustituye el control integral de contexto. Cambiar a un intervalo sin cruces no exige bajar umbrales. Un episodio persistente puede aparecer varias veces por sus distintos minutos: el panel no agrupa automáticamente incidentes.

### ID 5 — Volatilidad · observada y futura a 15 min

Gráfico temporal PostgreSQL, live y versión activa. Por activo representa volatilidad retrospectiva (`observed`), pronóstico (`forecast`) y volatilidad futura efectivamente realizada (`actual`, cuando exista). Las tres usan escala porcentual de volatilidad, sin anualizar. La realidad se dibuja en el ancla para comparar lo previsto en ese momento, aunque se conozca después.

Sirve para comprobar si se esperaba mucha o poca variación y cómo resultó, no para deducir dirección de compra/venta. Una diferencia entre observado y pronóstico no es por sí misma error: miden tramos distintos. El error compara forecast y actual del mismo ancla.

### ID 6 — Caída significativa · probabilidad a 30 min

Gráfico temporal PostgreSQL de `forecast` para ambos activos, live/versión/intervalo. Escala 0–1 formateada como porcentaje. El evento es rendimiento final bajo P5: aproximadamente −0,558 % BTC y −1,116 % SOL. No dibuja el rendimiento retrospectivo como si fuera otra probabilidad ni el mínimo intraintervalo.

Una probabilidad de 10 % no significa que se pronostique una pérdida de 10 %. El corte actual de vigilancia es cercano a 5,80 % de probabilidad. La calidad histórica del clasificador es limitada; consultar precisión y recall antes de atribuir significado fuerte a un pico.

### ID 7 — Actividad anómala · probabilidad a 5 min

Gráfico temporal PostgreSQL de probabilidad futura por activo. Estima que volumen o trades del próximo tramo superen sus P95 por activo y hora UTC. La anomalía actual se ve en el centro de señales y la tabla de actividad. Un valor de 60 % no dice cuánto volumen habrá, sino la probabilidad estimada del evento binario definido.

### ID 8 — SOL menos BTC · divergencia a 15 min

Gráfico temporal PostgreSQL de spread reciente, pronóstico y realidad futura. Unidad puntos porcentuales. Un valor +1 significa que SOL rindió un punto porcentual más que BTC en el tramo correspondiente; −1, uno menos. Ambos activos pueden subir y aun así divergir.

El pronóstico actual permanece en cero porque fue la referencia ganadora de validación. Una línea plana en cero es el método desplegado deliberadamente, no por sí sola un fallo del stream. La señal observada puede seguir identificando desacoplamientos reales y la tabla de modelos revela por qué no se despliega ridge.

### ID 9 — Correlación BTC / SOL · rendimientos 60 min

Gráfico PostgreSQL de `market_kpis.metrics.correlation_60m` en filas del par, filtrado por intervalo. Escala −1 a +1. Correlación alta significa asociación lineal reciente entre cambios de un minuto, no causalidad ni igualdad de rentabilidad. Correlación y divergencia no son opuestas: dos series pueden estar correlacionadas y tener distintas magnitudes de rendimiento.

Esta consulta de KPI no filtra por versión ni modo como las de predicciones. Sus filas se identifican por símbolo y ancla; las pruebas/operación deben mantener clara su procedencia. Una correlación nula matemáticamente indefinida no se rellena con cero.

### ID 99 — Actividad observada · referencia por hora UTC

Tabla PostgreSQL con la última fila por activo de `market_kpis`. Muestra cierre, USDT últimos cinco minutos, P95 monetario, operaciones, P95 de operaciones y Z volumen. El P95 contextualiza el nivel de actividad con la distribución del entrenamiento para ese horario; el Z cuantifica desviaciones respecto de media/desviación de la misma referencia.

Un Z alto no equivale a una probabilidad bajo normalidad demostrada ni identifica fraude. El ratio usado para activar señal es el máximo de ambos cocientes contra P95; esa regla se explica en el centro de señales. La tabla toma la última fila sin filtro de intervalo o versión; verificar frescura en la cabecera.

### ID 11 — Prueba histórica reservada · modelo contra referencia

Tabla PostgreSQL que une `test_deployed` y `test_baseline` de la versión activa. Muestra dimensión, método desplegado, casos, MAE, RMSE, RMSE referencia, PR-AUC, Brier, Brier referencia, precisión, recall y versión.

Las columnas no aplicables quedan nulas: por ejemplo, una regresión no tiene Brier binario. La muestra de test es enero–septiembre 2026, anclas cada 15 minutos. Las métricas están agregadas entre activos cuando el modelo es común. No se usa el intervalo de tres horas del dashboard para recalcular esta evaluación histórica fija.

Sirve para juzgar si el método aporta frente a una referencia y conocer fallos. Mayor precisión/recall o PR-AUC suele ser mejor; menor MAE/RMSE/Brier, mejor. La comparación debe ser dentro del mismo objetivo y muestra. Una celda MAE de volatilidad no se compara directamente con Brier de caída.

### ID 12 — Evaluación en vivo · activo, dimensión y período seleccionado

Tabla PostgreSQL, live/versión activa y anclas del intervalo elegido. Por dimensión/símbolo cuenta emitidas, evaluadas y pendientes. Para regresiones calcula MAE y RMSE entre forecast y actual; para clasificaciones, Brier. `MSE referencia` es el promedio cuadrático del baseline contra actual: en clasificación coincide con Brier de la referencia; en regresión está en unidad al cuadrado y no se compara directamente con una RMSE sin extraer raíz.

Los promedios SQL omiten actual nulo, de modo que pendientes no se convierten en cero error. Si no hay resultados maduros, la métrica queda vacía mientras el conteo explica la situación. El panel actual no recalcula precision/recall en vivo ni intervalos de confianza. Predice cada minuto, con muestras temporalmente dependientes, a diferencia del muestreo histórico de quince minutos.

### ID 13 — WebSocket · 1 conectado

Stat Prometheus `cryptopulse_producer_connected`. Da visibilidad al transporte. 1 no prueba que el archivo o los modelos avancen; 0 exige revisar si REST mantiene la continuidad y cuál es el error de conexión. Su color neutral no debe leerse como un semáforo completo de salud.

### ID 14 — Spark · edad de señal de vida

Stat Prometheus `cryptopulse_stream_heartbeat_age_seconds`, en segundos. Se espera que se mantenga bajo cuando el bucle supervisa las consultas. Más de 60 segundos merece revisión. Un valor bajo con predicciones viejas indica que el driver vive, pero hay otro problema de variables, datos o inferencia.

### ID 15 — Kafka · eventos pendientes

Stat Prometheus `sum(cryptopulse_kafka_checkpoint_lag)`, suma de ambos topics. Muestra atraso de offsets del archivo, no una cola de órdenes financieras ni número de velas faltantes únicas. La alerta usa el lag por topic, no solamente la suma visible. Puede aumentar durante recuperación y disminuir al ponerse al día.

### ID 16 — Escenario de costo · sin facturación

Stat Prometheus `cryptopulse_estimated_monthly_cost_usd`, formato monetario USD. Muestra el escenario configurable explicado antes. No se deriva de la actividad de trading ni representa un cargo cobrado al usuario.

### ID 17 — Spark · duración de micro-lotes

Gráfico Prometheus `cryptopulse_batch_duration_seconds`, una serie por query. Permite diferenciar archivo con inferencia de ventanas fijas/deslizantes. Comparar con 15 segundos de trigger y con lag; un pico de recuperación aislado no equivale a saturación sostenida. Los valores son del último progreso de cada consulta, no histogramas de latencia extremo a extremo.

### ID 18 — Kafka · avance acumulado por tópico

Gráfico Prometheus de `sum by(topic) (kafka_topic_partition_current_offset{topic=~"market.klines.*"})`. Agrega las tres particiones de cada mercado para que la leyenda identifique el topic. Una curva creciente demuestra nuevas posiciones; una plana puede deberse a la cadencia de cierres o a detención, que se distingue con las demás métricas. Su pendiente visual no convierte automáticamente el eje en mensajes/segundo.

### ID 1 — Histórico limpio · corte batch

Stat PostgreSQL `sum(candles)` de `batch_kpis`. El resultado registrado es 4.993.757 velas limpias del corte 2022-01 a 2026-09. Es estático hasta que se reprocesa/publica otro batch. No aumenta cada minuto con el feed vivo, porque son rutas y tablas distintas.

### ID 35 — Ventanas finalizadas · cobertura real

Tabla PostgreSQL agrupada por kind y símbolo, con último cierre finalizado, total de ventanas y cantidad cuyo conteo de velas es diferente de cinco. Diferencia fijas de cinco minutos y deslizantes de cinco con paso uno. Los conteos son del archivo completo de ventanas, no solo del intervalo del dashboard.

Un resultado finalizado puede estar incompleto por arranque, huecos o eventos tardíos. Este panel hace visible esa condición y evita interpretar `final=true` como cobertura perfecta. No verifica por sí solo que un pronóstico use las variables correctas: esa continuidad se controla en el otro recorrido.

### IDs 40–43 — Encabezados de sección

Agrupan centro de señales; cuatro dimensiones; evidencia de utilidad; operación. No ejecutan consultas ni producen cifras. Contarlos como indicadores inflaría la cantidad de métricas disponibles.

### Secuencia práctica de lectura y decisiones de revisión

Primero comprobar LISTO, 7/7 y cada activo con 61 velas y cero huecos. Después leer el centro de señales, identificando origen, unidad y antigüedad. Si hay un cruce, contrastar el valor con su umbral y revisar otras dimensiones: volatilidad alta sin caída tiene una interpretación diferente de caída con actividad extraordinaria. Consultar los gráficos para ver persistencia y la realidad posterior cuando haya madurado.

Si todo aparece NORMAL, significa que las reglas disponibles no cruzaron sus referencias. Si aparece DESACTUALIZADO, SIN EVALUAR o NO DISPONIBLE, revisar primero el pipeline: no hay base para concluir normalidad del mercado. Si un pronóstico se equivoca, registrarlo en evaluación no debe cambiarse retrospectivamente para que parezca correcto.
