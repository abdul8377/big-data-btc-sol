> Estado histórico: este documento conserva los resultados y hallazgos de su fecha. Las correcciones y la operación vigente están en [PRODUCTO_FINAL.md](PRODUCTO_FINAL.md).

# Informe del trabajo realizado por sesión en CryptoPulse

**Proyecto:** CryptoPulse.  
**Directorio de trabajo:** `D:\bigdata`.  
**Cobertura:** sesiones 1 a 12, unidades 1 y 2.  
**Fecha:** 26 de octubre de 2026, America/Lima.  
**Criterio:** capacidades funcionales adaptadas al brief de BTCUSDT y SOLUSDT, con evidencia de ejecución.

Este documento explica qué se añadió, qué procesa y qué resultado dejó cada sesión. El informe general `D:\bigdata\docs\INFORME_PROYECTO_CRYPTOPULSE.md` desarrolla la arquitectura, los paneles de Grafana, las métricas y la guía completa para levantar el proyecto.

La implementación no reprodujo los datasets de H&M, la Biblia, los microservicios de órdenes ni los sensores físicos de los ejemplos. Conservó las capacidades técnicas del curso y las aplicó a CryptoPulse. Algunas funciones compartidas se consolidaron durante la integración; la clasificación siguiente expresa a qué capacidad de sesión corresponde cada parte, no que cada archivo aislado se escribiera exclusivamente en un único momento.

## 1. Vista general del avance

| Sesión | Objetivo funcional | Resultado |
|---|---|---|
| S01 | Entorno y primera lectura real | Docker, Jupyter, Spark y muestra BTC/SOL |
| S02 | Transformar datos con PySpark | Lectura, funciones, agregaciones, ventanas, RDD y formatos |
| S03 | Preparar datos confiables | Calidad, cuarentena, huecos y Parquet particionado |
| S04 | Entrenar y evaluar | Cuatro dimensiones analíticas, candidatos y referencias guardados |
| S05 | Integrar el batch | Verificación funcional de unidad 1 |
| S06 | Publicar y consumir eventos | Kafka, topics, contrato y prueba real de ida y vuelta |
| S07 | Ingerir datos de mayor frecuencia | WebSocket de mercado con recuperación REST |
| S08 | Procesar con estado | Ventanas, watermark, deduplicación y checkpoints |
| S09 | Observar el pipeline | Prometheus, exportadores, reglas y costos ilustrativos |
| S10 | Aplicar modelos a eventos nuevos | Inferencia sin reentrenamiento por evento y evaluación diferida |
| S11 | Integrar BI y ML | Un tablero Grafana con 19 paneles |
| S12 | Cerrar la integración | Pruebas de recuperación, temporalidad e idempotencia; PASS |

La última comprobación integral posterior al cambio de contraseña se registró el **26 de octubre de 2026 a las 21:54:34 de Lima**, equivalente al 27 de octubre a las 02:54:34 UTC.

## 2. Sesión 1 — Arquitectura y entorno

[Guía de referencia S01](https://262bigdata.github.io/lambda26/sesiones/S01_Arquitectura_Big_Data_Lambda_Kappa/).

### Objetivo

Verificar que el motor de procesamiento funciona y decidir una arquitectura coherente con el proyecto.

### Qué se hizo

Se leyó el brief de CryptoPulse y se mantuvo la arquitectura Lambda: una ruta histórica para preparación y aprendizaje, otra para eventos nuevos y una salida compartida.

Se preparó una imagen basada en el entorno del curso con Python 3.10, OpenJDK 21, PySpark 4.2.0, Notebook 7.6.2 y pandas 2.3.3. Se añadió nbclient para poder ejecutar notebooks automáticamente.

Se crearon la configuración Compose, el script de inicio, un descargador y un notebook de verificación. El dataset mínimo pasó a ser una muestra real de velas BTCUSDT y SOLUSDT del 1 de enero de 2025: 1 440 por activo.

El proyecto se preparó inicialmente en el workspace de Documents/Codex. A solicitud del usuario se copió a `D:\bigdata`, se comprobaron los archivos y se recreó el contenedor para montar `D:\bigdata\pyspark\sesiones` en `/opt`.

### Qué se verificó

- Descarga real y hashes oficiales.
- 2 880 filas leídas por Spark.
- Una SparkSession activa y respuesta de Spark UI.
- Esquema y campos esenciales de las velas.
- Resultado funcional PASS.

El acceso inicial de la sesión restringida al motor Docker fue un bloqueo del entorno de ejecución, no una avería del Docker del usuario. Se resolvió usando ejecución con permisos adicionales.

### Archivos y aportación al producto

- `D:\bigdata\pyspark\Dockerfile`.
- `D:\bigdata\pyspark\compose.yml`.
- `D:\bigdata\scripts\iniciar_s01.ps1`.
- `D:\bigdata\scripts\descargar_muestra_s01.py`.
- `D:\bigdata\pyspark\sesiones\s01-arquitectura\01_arquitectura_practica.ipynb`.
- `D:\bigdata\pyspark\sesiones\s01-arquitectura\artifacts\verificacion_s01.json`.

Esta sesión dejó el entorno reutilizable para el resto del proyecto. El token de JupyterLab es `sintoken`.

## 3. Sesión 2 — Fundamentos de PySpark aplicados a CryptoPulse

[Guía de referencia S02](https://262bigdata.github.io/lambda26/sesiones/S02_Fundamentos_PySpark_Transformaciones_Lazy_Evaluation/).

### Objetivo

Construir las primeras transformaciones reales y comprender cuándo Spark solo define un plan y cuándo ejecuta trabajo.

### Qué se añadió

Se creó el descargador mensual parametrizable con verificación de ZIP y CSV, manifiesto de procedencia, caché y selección de meses cerrados.

Para la primera ejecución se descargaron diciembre de 2024 y enero de 2025 para ambos activos: cuatro archivos y **178 560 velas**. Ese intervalo prueba tanto la escala de milisegundos como la de microsegundos.

El notebook implementó:

- Lectura por inferencia y lectura con esquema explícito.
- Selecciones y filtros.
- Conversión temporal UTC.
- Columnas de fecha, hora, tipo de día y dirección de vela.
- Rendimientos y volatilidad realizada.
- Agregaciones por activo y fecha.
- OHLC horario ordenado por tiempo, VWAP y conteo de operaciones.
- Muestra reproducible, escritura y relectura CSV/Parquet.
- RDD con `map`, `filter`, `flatMap` y `reduceByKey`, contrastado con DataFrame.

### Qué se comprobó

La construcción de transformaciones no creó nuevos jobs; una acción `count()` sí los creó. Se conservó el plan de ejecución.

Las agregaciones horarias se contrastaron contra 60 filas originales. Se verificaron continuidad, hashes, transición temporal 2024/2025, lectura de archivos y coincidencia RDD/DataFrame.

Una comprobación independiente detectó pérdida de precisión al dividir columnas Decimal antes del logaritmo. Se corrigió convirtiendo los operandos derivados a doble precisión, sin cambiar los precios originales. También se ajustó el calentamiento de las ventanas para no publicar resultados parciales como si cubrieran todo el horizonte.

### Archivos y alcance de la evidencia

- `D:\bigdata\scripts\descargar_historicos_s02.py`.
- `D:\bigdata\scripts\preparar_s02.ps1`.
- `D:\bigdata\scripts\verificar_s02.ps1`.
- `D:\bigdata\pyspark\sesiones\s02-fundamentos\02_fundamentos_practica.ipynb`.
- `D:\bigdata\pyspark\sesiones\s02-fundamentos\artifacts\02_fundamentos_practica_ejecutado.ipynb`.
- Carpetas de muestras, datos transformados y resúmenes en los artefactos de S02.

La verificación inicial de dos meses produjo PASS. En la inspección para este informe, el notebook ejecutado conserva celdas sin salidas de error; el archivo JSON suelto de verificación S02 no está presente. No se recreó un PASS artificial para sustituirlo.

El manifiesto de datos compartido se amplió después a 2022–2026 para S03 y S04. Por eso el intervalo actual del manifiesto y la cifra de la primera verificación S02 no son el mismo corte.

## 4. Sesión 3 — Calidad y almacenamiento analítico

[Guía de referencia S03](https://262bigdata.github.io/lambda26/sesiones/S03_Procesamiento_Calidad_Datos_Particionamiento/).

### Objetivo

Dejar un dataset con reglas explícitas de validez, trazabilidad de exclusiones y almacenamiento eficiente.

### Qué se hizo

Se descargaron los 114 archivos mensuales del periodo enero de 2022 a septiembre de 2026. El módulo de calidad verifica hashes, utiliza esquema explícito y normaliza los tiempos.

Se aplicaron controles de:

- Campos requeridos y símbolos.
- Precios positivos y consistencia OHLC.
- Volumen, importe y operaciones no negativos.
- Alineación temporal y duración de vela.
- Duplicados por activo e inicio.
- Versiones conflictivas de una misma vela.
- Huecos después de la depuración.

Se guardó cuarentena en lugar de eliminar sin rastro. Los datos válidos se escribieron en Parquet por activo, año y mes. Se registraron los huecos sin imputar precios.

### Resultado comprobado

| Control | Resultado |
|---|---:|
| Filas de entrada | 4 993 759 |
| Filas válidas | 4 993 757 |
| Intervalos inválidos en cuarentena | 2 |
| Claves con conflicto de valores | 0 |
| Retiro de filas por duplicados o conflictos | 0 |
| Huecos | 3 |
| Minutos faltantes sumados por activo | 163 |

Los dos intervalos inválidos tienen inicio 2023-03-24 12:39 UTC, uno por activo. El campo que motivó la exclusión fue el intervalo, no la ausencia de precios.

Se verificó una consulta real con `PartitionFilters` en el plan físico. La salida se leyó de vuelta y sus conteos se comprobaron.

### Código y salidas

- `D:\bigdata\pyspark\sesiones\cryptopulse\quality.py`.
- `D:\bigdata\pyspark\sesiones\s03-calidad\artifacts\clean_parquet`.
- Carpetas `quarantine` y `gaps` bajo los artefactos de S03.
- `D:\bigdata\pyspark\sesiones\_runtime\s03.json`.

Esta sesión proporcionó la entrada confiable para los modelos y dejó evidencia del costo de excluir un registro: la serie puede necesitar recalentar sus ventanas al atravesar un hueco.

## 5. Sesión 4 — Modelos distribuidos y evaluación temporal

[Guía de referencia S04](https://262bigdata.github.io/lambda26/sesiones/S04_ML_Distribuido_Regresion_Spark_MLlib/).

### Objetivo

Entrenar modelos reutilizables y comprobar si aportan valor frente a una referencia simple.

### Qué se añadió

Se creó un módulo común de variables para evitar diferencias entre entrenamiento e inferencia. Calcula rendimientos, volatilidad realizada, actividad reciente, rango, cuerpo, estacionalidad horaria y variables conjuntas BTC–SOL.

Se prepararon cuatro objetivos:

1. Volatilidad futura a 15 minutos.
2. Caída al cabo de 30 minutos.
3. Actividad anómala de los próximos cinco minutos.
4. Diferencia de rendimientos SOL menos BTC a 15 minutos.

Se ajustaron cinco candidatos ML para cuatro dimensiones: dos candidatos para volatilidad, un clasificador de caídas, uno de anomalías y un regresor de divergencia.

Se empleó 2022–2024 para entrenamiento, 2025 para validación y 2026 hasta septiembre para prueba. Se purgaron los 30 minutos finales antes de los límites principales. Dentro de validación, los clasificadores usan un tramo para calibración y otro para selección.

### Hallazgos y correcciones

El cálculo de correlación encontró ventanas de varianza cero. Se sustituyó la división que fallaba por una división tolerante que conserva NULL cuando la correlación no puede definirse.

Los periodos no se mezclaron aleatoriamente. Las etiquetas futuras se limitaron a aprendizaje y evaluación. Las filas de entrenamiento se muestrearon cada 15 minutos después de construir las variables sobre la serie de un minuto.

### Qué se seleccionó

| Dimensión | Método elegido | Criterio |
|---|---|---|
| Volatilidad | Random Forest | Menor RMSE de validación |
| Caídas | Regresión logística calibrada | Menor Brier frente a frecuencia histórica |
| Anomalías | Regresión logística calibrada | Menor Brier frente a frecuencia histórica |
| Divergencia | Baseline cero | Ridge no mejoró el RMSE de referencia |

Los modelos guardados se recargaron para puntuar el conjunto de prueba. Se conservaron predicciones, métricas, umbrales, versiones y métodos no seleccionados.

La referencia de divergencia no es un modelo omitido: es una decisión de selección explícita respaldada por la comparación. Las probabilidades de clasificación tampoco se presentan como aciertos garantizados.

### Código y salidas

- `D:\bigdata\pyspark\sesiones\cryptopulse\features.py`.
- `D:\bigdata\pyspark\sesiones\cryptopulse\training.py`.
- `D:\bigdata\pyspark\sesiones\s04-modelos\artifacts\model_registry.json`.
- Carpetas `models`, `model_dataset`, `pair_dataset` y `test_predictions` bajo S04.
- `D:\bigdata\pyspark\sesiones\_runtime\s04.json`.

La versión inspeccionada es `20261026T223241Z`. Las métricas detalladas se incluyen en el informe general.

## 6. Sesión 5 — Integración del producto batch de unidad 1

[Guía de referencia S05](https://262bigdata.github.io/lambda26/sesiones/S05_Evaluacion_Unidad_1/).

### Objetivo

Comprobar que los componentes batch forman un recorrido integrado y persistido.

### Qué se hizo

Se implementó `verify_u1()` en el módulo de entrenamiento. Comprueba la verificación de calidad, la existencia del dataset válido, las cuatro dimensiones, los metadatos de modelos guardados, las particiones con filas y las métricas de prueba.

### Resultado y aportación

El cierre produjo **PASS** sobre 4 993 757 filas de calidad y cuatro dimensiones persistidas y evaluadas. Confirmó que la plataforma podía pasar de datos públicos a salidas analíticas y artefactos de modelo.

No se realizó una prueba teórica ni se asignó una calificación de la asignatura. Este cierre es funcional.

### Reproducción

~~~powershell
Set-Location D:\bigdata
docker exec -e PYTHONPATH=/opt cryptopulse-pyspark python -m cryptopulse.training --verify-u1
~~~

El registro está en `D:\bigdata\pyspark\sesiones\_runtime\s05.json`.

## 7. Sesión 6 — Kafka y contrato de eventos

[Guía de referencia S06](https://262bigdata.github.io/lambda26/sesiones/S06_Ingesta_Eventos_Empresariales_Kafka/).

### Objetivo

Transmitir eventos de mercado con un contrato que pueda validar un consumidor independiente.

### Qué se añadió

Se configuró Kafka en modo KRaft, Kafka UI y una red Docker propia. Se crearon los topics de BTC y SOL con tres particiones, además de tópicos separados para pruebas.

El evento de negocio adaptado es `market.candle.closed`, versión de esquema 1. Su clave temporal identifica activo, intervalo e inicio. El contrato incluye precios, actividad, tiempos, confirmación de cierre, modo de origen y transporte.

Se añadió el esquema JSON y una función ejecutable que valida antes de publicar y al recibir. La política de publicación espera confirmación del broker.

### Qué se comprobó

El verificador publicó un mensaje de prueba con identificador único y lo leyó en la partición y offset devueltos por Kafka. Esta prueba se realiza fuera de los topics de mercado.

Se comprobó también la validación del esquema sobre una vela histórica real. El despliegue usa un broker y no ofrece replicación de alta disponibilidad.

### Archivos

- `D:\bigdata\platform.yml`.
- `D:\bigdata\schemas\market-kline-v1.schema.json`.
- `D:\bigdata\pyspark\sesiones\cryptopulse\contract.py`.
- `D:\bigdata\pyspark\sesiones\cryptopulse\verify.py`, opción `6`.
- `D:\bigdata\pyspark\sesiones\_runtime\s06.json`.

## 8. Sesión 7 — Ingesta continua de telemetría de mercado

[Guía de referencia S07](https://262bigdata.github.io/lambda26/sesiones/S07_Ingesta_Eventos_IoT_Sensores_Tiempo_Real/).

### Adaptación al brief

La guía utiliza sensores IoT. CryptoPulse necesita cotizaciones y actividad de mercado. Se adaptó la capacidad de ingesta continua al WebSocket público de Binance; no se crearon lecturas de temperatura, dispositivos falsos ni un flujo MQTT ajeno al proyecto.

Esta adaptación implementa la funcionalidad del producto y no acredita por sí sola un requisito académico específico de hardware físico.

### Qué se hizo

Se construyó un productor separado del notebook. Al iniciar, recupera contexto con REST; después escucha ambos streams WebSocket. Solo publica cierres confirmados.

Se añadieron recuperación periódica por REST, reconexión con espera creciente, validación, identificadores estables, clave de particionado por activo y estado persistido del último inicio publicado.

Los modos `live` y `backfill` distinguen un evento reciente del contexto recuperado. No equivalen a dato real frente a dato simulado: ambos usan velas reales.

### Qué se verificó

Se recibió tráfico WebSocket, se publicaron cierres reales, se consumió un último evento de cada topic y se verificaron contrato, símbolo, clave, partición y offset. La comprobación identificó mensajes cuyo transporte era WebSocket.

### Código y estado

- `D:\bigdata\pyspark\sesiones\cryptopulse\producer.py`.
- Servicio `producer` de Compose.
- `D:\bigdata\pyspark\sesiones\_runtime\producer_status.json`.
- `D:\bigdata\pyspark\sesiones\_runtime\s07.json`.

## 9. Sesión 8 — Ventanas watermark y recuperación

[Guía de referencia S08](https://262bigdata.github.io/lambda26/sesiones/S08_Procesamiento_Streaming_Spark_Ventanas_Watermarking/).

### Objetivo

Consumir continuamente con semántica temporal definida y capacidad de retomar después de una interrupción.

### Qué se implementó

El proceso `streaming` usa el conector oficial Spark–Kafka y tres consultas:

1. Archivo validado en PostgreSQL.
2. Ventanas fijas de cinco minutos.
3. Ventanas deslizantes de cinco minutos con paso de un minuto.

Las consultas de ventanas usan watermark de dos minutos y deduplicación por identificador. Se guardan offsets, commits y estado en checkpoints de `D:\bigdata\pyspark\sesiones\_runtime\checkpoints`.

El archivo principal conserva eventos válidos aunque una consulta de ventanas los considere demasiado tardíos. Las claves de PostgreSQL impiden duplicados persistidos.

### Qué se comprobó

La verificación revisó suficientes velas por activo, filas de los dos tipos de ventana y existencia de commits y estado.

En el cierre integrado se ejecutó además esta secuencia aislada:

- Se envió dos veces una vela real: quedó una fila.
- Se envió otra diez minutos posterior: quedaron dos.
- Se envió un nuevo evento anterior al watermark: siguieron dos.
- Se detuvo la consulta.
- Se envió otro evento y se reinició con el mismo checkpoint y sink: quedaron tres.

Se verificó al menos una fila descartada por watermark. La prueba se realizó con un topic exclusivo, sin sustituir precios del flujo en vivo.

### Aportación y límites

Quedó comprobada la entrega al menos una vez con escrituras idempotentes, no una transacción global exactamente una vez. El trigger de 15 segundos es una configuración de procesamiento; no cambia la frecuencia de cierre de las velas.

Código: `D:\bigdata\pyspark\sesiones\cryptopulse\stream.py`. Evidencia: `D:\bigdata\pyspark\sesiones\_runtime\s08.json` e `integration_tests.json`.

## 10. Sesión 9 — Observabilidad y costos

[Guía de referencia S09](https://262bigdata.github.io/lambda26/sesiones/S09_Observabilidad_Pipeline_Grafana_Costos/).

### Objetivo

Distinguir un pipeline sano de otro que dejó de recibir, procesar o actualizar información.

### Qué se añadió

Se levantaron Prometheus, Kafka exporter, el exportador propio de CryptoPulse y Grafana.

El exportador proporciona conexión WebSocket, heartbeats, eventos publicados, antigüedad de velas, duración de lotes, ritmo de entrada y descartes por watermark. El lag se estima comparando offsets del broker con los procesados por Spark, en lugar de asumir que el consumidor Spark confirma offsets de un consumer group convencional.

Se configuraron cinco reglas operativas: exportador no disponible, velas antiguas, heartbeat Spark detenido, lag alto y ausencia de mensajes WebSocket.

### Costos

Se añadió una calculadora parametrizable de CPU, RAM y almacenamiento. Los supuestos iniciales producen 65.145 USD/mes, expresamente como ejemplo ilustrativo. No es una factura del equipo ni un precio actual de un proveedor.

### Qué se verificó y dónde está

Los tres targets de Prometheus estuvieron `up` y las cinco reglas se cargaron. La configuración está en:

- `D:\bigdata\observability\prometheus.yml`.
- `D:\bigdata\observability\alerts.yml`.
- `D:\bigdata\pyspark\sesiones\cryptopulse\exporter.py`.
- `D:\bigdata\config\cost-assumptions.json`.
- `D:\bigdata\scripts\actualizar_costos.ps1`.
- `D:\bigdata\pyspark\sesiones\_runtime\s09.json`.

No se configuró un canal externo de envío de alertas.

## 11. Sesión 10 — Inferencia en series de tiempo

[Guía de referencia S10](https://262bigdata.github.io/lambda26/sesiones/S10_Series_Tiempo_Inferencia_Streaming_MLlib/).

### Objetivo

Reutilizar los modelos validados sobre información nueva y medir el pronóstico cuando se conozca el futuro.

### Qué se implementó

El bundle carga modelos y umbrales del registro. Cada micro-lote con eventos nuevos consulta hasta 180 velas recientes por activo, calcula variables con las funciones comunes y selecciona el último contexto válido.

Se generan las siete salidas del sistema sin reentrenar los modelos. Se guardan emisión, horizonte, versión, método, valor pronosticado, baseline, indicador observado y bandera de alerta.

La verdad futura se calcula solo cuando se dispone del cierre final y de todas las velas consecutivas necesarias. Entonces se completan `actual` y `evaluated_at`.

### Qué se comprobó

Se verificó que el streaming tenía inferencia habilitada y que las cuatro dimensiones estaban presentes en las siete combinaciones esperadas.

Las pruebas posteriores generaron siete predicciones replay reales y evaluaron sus resultados. Cambiar los cierres posteriores al momento de emisión no modificó las variables anteriores. Insertar otra vez los mismos pronósticos no aumentó la cantidad de filas.

En la captura actual las cuatro dimensiones ya cuentan con predicciones live evaluadas, además de las de replay.

### Código

- `D:\bigdata\pyspark\sesiones\cryptopulse\inference.py`.
- `D:\bigdata\pyspark\sesiones\cryptopulse\db.py`.
- `D:\bigdata\pyspark\sesiones\cryptopulse\features.py`.
- `D:\bigdata\pyspark\sesiones\_runtime\s10.json`.

La divergencia usa el baseline elegido en validación. Que su valor sea cero no es una falla del motor ni una garantía de que ambos activos no divergirán.

## 12. Sesión 11 — BI y visualización integrada

[Guía de referencia S11](https://262bigdata.github.io/lambda26/sesiones/S11_BI_ML_Distribuido_Spark_Grafana/).

### Objetivo

Unir los indicadores históricos, las predicciones y la salud técnica en una historia consultable.

### Qué se añadió

Se publicaron en PostgreSQL **3 468 resúmenes diarios** correspondientes al histórico válido y **12 filas de métricas**: cuatro dimensiones por las variantes de comparación candidato, referencia y método desplegado en prueba.

Se configuraron una fuente PostgreSQL de solo lectura y una fuente Prometheus en Grafana. El tablero unificado tiene 19 paneles: 13 consultan PostgreSQL y seis usan Prometheus.

Los paneles muestran precios de cierre, frescura, cuatro dimensiones, correlación, alertas, evaluación histórica, evaluación en vivo, actividad y percentiles, estado de conexión, lag, duración de lotes y costo ilustrativo.

No se usó Pushgateway para publicar las predicciones persistentes: los resultados de negocio se consultan en PostgreSQL y las métricas operativas se exponen mediante scraping.

### Qué se corrigió

Las macros de tiempo de Grafana no interpretaron correctamente una expresión anidada. Se sustituyeron por límites de epoch sobre el campo en milisegundos. También se forzó aritmética de 64 bits para evitar el desbordamiento al multiplicar segundos por mil.

### Qué se verificó

Se comprobó la salud de ambas fuentes y se ejecutaron las 13 consultas SQL de los paneles. No se dio por válida una petición HTTP fallida solo porque no hubiera un campo de error en una respuesta incompleta.

### Archivos

- `D:\bigdata\pyspark\sesiones\cryptopulse\publish_batch.py`.
- `D:\bigdata\observability\grafana\provisioning`.
- `D:\bigdata\observability\grafana\dashboards\cryptopulse.json`.
- `D:\bigdata\pyspark\sesiones\_runtime\s11.json`.

## 13. Sesión 12 — Verificación integral de unidad 2

[Guía de referencia S12](https://262bigdata.github.io/lambda26/sesiones/S12_Evaluacion_Unidad_2/).

### Objetivo

Comprobar el recorrido completo desde fuente pública a visualización y recuperación.

### Qué se hizo

Se creó una batería de integración con velas históricas reales en un topic separado. También se creó un verificador del producto que consulta los servicios y los registros de las sesiones anteriores.

### Comprobaciones de integración

| Prueba | Resultado |
|---|---|
| Evento real válido | Aceptado |
| Seis variantes inválidas | Rechazadas |
| Duplicado | Contado una vez |
| Evento anterior al watermark | Descartado |
| Recuperación de checkpoint y sink | Correcta |
| Cuatro dimensiones en replay | Siete pronósticos generados y evaluados |
| Modificar futuro sin alterar variables pasadas | Correcto |
| Repetir inserción de pronósticos | Sin duplicar filas |

### Comprobaciones del producto

Se revisaron los PASS de S03 a S11, tráfico de ambos activos, ventanas, checkpoints, inferencia, frescura, duplicados, targets, reglas, fuentes de Grafana, consultas de paneles y resultado de integración.

El verificador reutiliza la evidencia de integración guardada; ejecutar solo `verificar_producto.ps1` no vuelve a ejecutar todos los escenarios de la batería ni reentrena.

### Resultado

**S12: PASS.** El registro más reciente inspeccionado es posterior al cambio de contraseña de Grafana y muestra resultados futuros live evaluados en las cuatro dimensiones.

La comprobación es funcional. No sustituye una sustentación individual, una evaluación teórica ni la aprobación del docente.

### Código y artefactos

- `D:\bigdata\pyspark\sesiones\cryptopulse\integration.py`.
- `D:\bigdata\pyspark\sesiones\cryptopulse\verify.py`.
- `D:\bigdata\scripts\pruebas_integracion.ps1`.
- `D:\bigdata\scripts\verificar_producto.ps1`.
- `D:\bigdata\pyspark\sesiones\_runtime\integration_tests.json`.
- `D:\bigdata\pyspark\sesiones\_runtime\s12.json`.

## 14. Guía rápida para iniciar y revisar el resultado

La guía completa de requisitos, reconstrucción, entrenamiento y solución de problemas está en el informe general.

### Inicio habitual

1. Abrir Docker Desktop y esperar al motor Linux.
2. Abrir PowerShell.
3. Ejecutar:

~~~powershell
Set-Location D:\bigdata

powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\iniciar_producto.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verificar_producto.ps1
~~~

Si se acaba de iniciar desde frío, esperar al contexto de datos y al cierre de una vela antes de considerar definitiva una primera comprobación.

### Acceso

- [Tablero Grafana](http://localhost:43000/d/cryptopulse-main): usuario **admin**, contraseña **admin2026!**.
- [Kafka UI](http://localhost:48085).
- [Prometheus](http://localhost:49090).
- [Estado JSON](http://localhost:48000/api/status).
- [JupyterLab](http://localhost:4488/lab?token=sintoken): token **sintoken**.

### Logs

~~~powershell
docker compose -f .\platform.yml ps
docker compose -f .\platform.yml logs --tail 100 producer
docker compose -f .\platform.yml logs --tail 100 streaming
~~~

### Detención sin eliminar datos

~~~powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\detener_producto.ps1
~~~

Los notebooks son un servicio separado:

~~~powershell
docker compose -f .\pyspark\compose.yml stop
~~~

## 15. Qué se entregó y qué queda fuera

Se entregó un producto local que integra las capacidades funcionales previstas hasta la unidad 2: preparación histórica, modelos evaluados, ingesta pública, streaming con estado, persistencia, observabilidad y BI.

Los resultados no equivalen a haber ejecutado hardware IoT, desplegado un clúster de alta disponibilidad o completado las sesiones 13 a 16. Tampoco se añadieron capturas, diapositivas, asignaciones individuales ni calificaciones, porque el alcance solicitado priorizó el producto.

La configuración de arranque, los scripts y el código permanecen en `D:\bigdata`. Los informes describen el estado implementado y dejan explícitas las diferencias frente a los ejemplos del curso.

## 16. Evidencias consultadas

- Brief técnico analítico CryptoPulse proporcionado por el equipo.
- [Sitio del curso](https://262bigdata.github.io/lambda26/).
- [Repositorio del curso](https://github.com/262bigdata/lambda26), base consultada `973ac3020b573a7c66fd925f7d07a9f2553411ec`.
- Notebook y verificación inicial S01.
- Notebook ejecutado de S02 y artefactos presentes.
- Manifiesto histórico 2022–2026 y controles de S03.
- Registro de modelos y predicciones de prueba de S04.
- JSON funcionales de S03 a S12.
- Estado de los servicios, consultas PostgreSQL y configuración real de Grafana.
- `D:\bigdata\pyspark\sesiones\_runtime\report_snapshot.json`.
- `D:\bigdata\pyspark\sesiones\_runtime\report_evidence.json`.

Los contadores live se atribuyen al corte operativo indicado en el informe general. Los resultados históricos y los PASS se presentan con su alcance, sin convertir pruebas funcionales en aprobaciones académicas.
