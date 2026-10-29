# CryptoPulse

La versión operativa corregida y su guía de uso están en [docs/PRODUCTO_FINAL.md](docs/PRODUCTO_FINAL.md). El tablero distingue **NORMAL**, **VIGILANCIA**, **ALERTA**, **REFERENCIA** y datos ausentes o atrasados. No genera alertas artificiales para llenar una tabla. `/health` comprueba continuidad y siete pronósticos recientes; `/api/signals` explica las evaluaciones actuales.

Plataforma funcional de análisis histórico y en vivo para BTCUSDT y SOLUSDT en Binance Spot. Proyecto activo en `D:\bigdata`, adaptado al brief del equipo y a las capacidades de las sesiones 1 a 12 del curso.

## Acceder al producto

- [Grafana y tablero unificado](http://localhost:43000/d/cryptopulse-main): usuario `admin`, contraseña actual `admin2026!`.
- [Kafka UI](http://localhost:48085).
- [Prometheus](http://localhost:49090).
- [Estado del pipeline en JSON](http://localhost:48000/api/status).
- [JupyterLab](http://localhost:4488/lab?token=sintoken): token `sintoken`.

Los servicios publicados en Windows escuchan en localhost. No se requieren claves API de Binance ni de servicios de IA. El proyecto consume datos públicos y produce indicadores y señales de riesgo; no ejecuta órdenes.

## Iniciar detener y verificar

Desde PowerShell:

~~~powershell
Set-Location D:\bigdata
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\iniciar_producto.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verificar_producto.ps1
~~~

Para detener solo la plataforma, conservando volúmenes, modelos y checkpoints:

~~~powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\detener_producto.ps1
~~~

El entorno de notebooks se gestiona por separado con `scripts/iniciar_s01.ps1` y `docker compose -f pyspark/compose.yml stop`. La imagen de runtime amplía la imagen PySpark del curso; el script de inicio construye las imágenes que falten.

## Flujo implementado

**Batch**: ZIP mensuales públicos con SHA256 → CSV originales → controles PySpark → cuarentena y registro de huecos → Parquet por activo año y mes → variables temporales → entrenamiento MLlib → validación temporal → modelos y umbrales versionados.

**Tiempo real**: WebSocket de velas cerradas, con recuperación REST → Kafka → tres consultas Spark Structured Streaming → PostgreSQL → Grafana. Una consulta archiva eventos de forma idempotente; las otras calculan ventanas fijas de cinco minutos y deslizantes de cinco minutos con paso de un minuto. Las ventanas usan watermark de dos minutos y deduplicación por identificador.

El archivo durable conserva los eventos válidos aunque sean demasiado tardíos para una ventana de tiempo real. Los reintentos no duplican una vela o una predicción porque PostgreSQL aplica claves únicas. Esto proporciona resultados idempotentes sobre entrega al menos una vez; no se presenta como una transacción global exactamente una vez.

Los datos de mercado sustituyen el caso de sensores de la guía. No se añaden sensores físicos ficticios al brief de CryptoPulse.

## Dimensiones analíticas

| Dimensión | Salida | Horizonte | Método desplegado |
|---|---|---|---|
| Volatilidad | Volatilidad realizada futura en porcentaje sin anualizar | 15 minutos | Random Forest |
| Caídas | Probabilidad calibrada de rendimiento bajo el umbral histórico del activo | 30 minutos | Regresión logística |
| Actividad anómala | Probabilidad de actividad futura sobre referencias de volumen y operaciones por activo y hora UTC | 5 minutos | Regresión logística |
| Divergencia | Rendimiento SOL menos BTC en puntos porcentuales | 15 minutos | Referencia de divergencia cero |

Se entrenó también un regresor de divergencia. No mejoró la referencia en validación, por lo que la política conservó el baseline. Las métricas del candidato y de la referencia permanecen disponibles en el registro de modelos. El tablero muestra el método que está usando el producto.

La volatilidad retrospectiva y futura se define como 100 por la raíz de la suma de cuadrados de 15 rendimientos logarítmicos de un minuto, sin anualizar. Los precios originales se conservan como decimales y los cocientes derivados se calculan en doble precisión. Las variables solo utilizan datos disponibles al cierre de la vela.

## Datos y evaluación

Se descargaron **114 archivos mensuales**, desde enero de 2022 hasta septiembre de 2026: **4 993 759 velas reales**. S03 conservó **4 993 757 registros válidos**, puso dos registros en cuarentena y registró tres huecos que suman 163 minutos ausentes entre los dos activos. No se imputan precios ni se fabrican velas.

- Entrenamiento: 2022 a 2024.
- Validación: 2025.
- Calibración de clasificadores: enero a agosto de 2025; selección y umbrales de alerta: septiembre a diciembre de 2025.
- Prueba: enero a septiembre de 2026.
- Embargo temporal: 30 minutos entre particiones para que las etiquetas futuras no atraviesen el límite.
- Los modelos se entrenan y evalúan con observaciones cada 15 minutos. Las variables y etiquetas se calculan sobre velas de un minuto y la inferencia se actualiza en cada cierre de un minuto.

Los umbrales de caídas usan el percentil 5 del rendimiento futuro de 30 minutos del entrenamiento por activo. Las referencias de actividad usan percentiles 95 de ventanas de cinco minutos por activo y hora UTC. Los horizontes futuros solo se utilizan como etiquetas offline y para evaluar predicciones cuyo horizonte ya maduró.

Las predicciones en vivo se registran al emitirse. La evaluación incorpora su resultado real después de 5, 15 o 30 minutos, cuando hay suficientes velas completas y consecutivas. Las pruebas históricas aceleradas se marcan como `replay` y se excluyen de las gráficas y métricas de predicción en vivo.

## Sesiones y código

| Sesión | Capacidad implementada | Código o resultado principal |
|---|---|---|
| S01 | Docker Jupyter Spark y lectura real | `pyspark/sesiones/s01-arquitectura/` |
| S02 | Transformaciones agregaciones evaluación perezosa RDD y formatos | `pyspark/sesiones/s02-fundamentos/` |
| S03 | Calidad cuarentena huecos y Parquet particionado | `pyspark/sesiones/cryptopulse/quality.py` |
| S04 | Entrenamiento evaluación calibración y guardado MLlib | `pyspark/sesiones/cryptopulse/training.py` |
| S05 | Validación funcional del batch integrado | `training.py --verify-u1` |
| S06 | Kafka y contrato versionado con publicación y consumo verificados | `contract.py`, `schemas/`, `verify.py 6` |
| S07 | Ingesta real de los dos activos con WebSocket y recuperación REST | `producer.py` |
| S08 | Streaming ventanas watermark deduplicación y checkpoints | `stream.py` |
| S09 | Prometheus alertas operativas y estimación configurable de costos | `exporter.py`, `observability/` |
| S10 | Inferencia continua y evaluación al madurar cada horizonte | `inference.py` |
| S11 | KPIs batch y predicciones en un Grafana compartido | `publish_batch.py`, `observability/grafana/` |
| S12 | Verificación de integración y recuperación | `integration.py`, `verify.py 12` |

Los módulos anteriores están bajo `pyspark/sesiones/cryptopulse/`, salvo las carpetas señaladas desde la raíz. Las verificaciones por sesión se guardan en `pyspark/sesiones/_runtime/s03.json` hasta `s12.json`.

## Reproducir el batch y las pruebas

El corte de datos está en `config/pipeline.json`. Para descargar nuevamente, aplicar calidad y reentrenar:

~~~powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\entrenar_producto.ps1 -Descargar -Reprocesar -Reentrenar
~~~

El script pausa solo el consumidor streaming durante el entrenamiento y lo restaura después. El productor y Kafka siguen conservando los eventos. Los modelos nuevos se publican mediante un registro atómico al terminar la evaluación.

Para actualizar las salidas batch en PostgreSQL:

~~~powershell
docker compose -f platform.yml run --rm --no-deps tools python -m cryptopulse.publish_batch
~~~

Para probar duplicados, datos tardíos, recuperación de checkpoint y la evaluación de las cuatro dimensiones:

~~~powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\pruebas_integracion.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verificar_producto.ps1
~~~

Las pruebas usan un tópico separado y velas históricas reales. Comprueban que un duplicado se cuenta una sola vez, que un dato anterior al watermark se descarta, que el sink se recupera con su checkpoint y que modificar datos futuros no cambia las variables del pasado.

## Persistencia y operación

- Código configuración históricos Parquet modelos y checkpoints: `D:\bigdata`.
- PostgreSQL Kafka Prometheus y Grafana usan volúmenes administrados por Docker. El directorio físico de esos volúmenes depende de la ubicación del disco virtual de Docker Desktop.
- `.env` contiene las credenciales locales del despliegue y está excluido de Git.
- Los logs se consultan con `docker compose -f platform.yml logs --tail 100 streaming`, sustituyendo el servicio cuando corresponda.
- El job de streaming necesita el conector oficial Spark Kafka 4.2.0. La primera ejecución descarga sus JAR y los conserva en `pyspark/sesiones/_runtime/ivy/`.
- La configuración actual usa un broker y Spark local dentro de contenedores. Permite ejecutar el producto en este equipo; no es un clúster de alta disponibilidad.

Los costos son una **estimación ilustrativa editable**, no una factura ni una cotización de un proveedor. Cambia `config/cost-assumptions.json` y ejecuta `scripts/actualizar_costos.ps1` para recalcularlos.

## Referencias

- Brief técnico analítico CryptoPulse entregado por el equipo.
- [Guía del curso](https://262bigdata.github.io/lambda26/).
- [Repositorio de referencia](https://github.com/262bigdata/lambda26), base consultada `973ac3020b573a7c66fd925f7d07a9f2553411ec`.
- [Binance Public Data](https://github.com/binance/binance-public-data).
- [Binance Market Data Only](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md).
- [Spark Structured Streaming y Kafka](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html).

## Informes detallados

- Producto actual, correcciones, pruebas y criterios de interpretación: `docs/PRODUCTO_FINAL.md`.
- Inicio local paso a paso: `docs/GUIA_INICIO_LOCAL.md`.

- Informe del proyecto, paneles y guia operativa: D:\bigdata\docs\INFORME_PROYECTO_CRYPTOPULSE.md
- Trabajo y resultados por sesion: D:\bigdata\docs\INFORME_SESIONES_S01_S12.md
