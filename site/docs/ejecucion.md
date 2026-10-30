# Ejecutar y reproducir CryptoPulse

!!! note "Sitio estático y producto local"
    Los puertos localhost de esta guía funcionan en el ordenador donde se ejecuta Docker. GitHub Pages publica documentación y no aloja los contenedores. Los accesos se consultan en la configuración local; no es necesario proporcionar claves Binance.

## Último arranque documentado

El **30/10/2026** se levantaron producto y Jupyter; integración y S12 pasaron con la versión `20261029T011759858407Z.cal`. Se publicaron [14 capturas reales y el registro de ejecución](evidencias.md). Se conservaron históricos/modelos de Windows y se recrearon los servicios en el motor disponible. Esta evidencia es fechada; para comprobar la instancia actual consultar `/health` y `/api/status`.

## Operación del proyecto: arranque, verificación y parada

### Requisitos y qué necesita estar presente

Para usar la copia actual se necesita Windows con Docker Desktop en modo de contenedores Linux, Docker Compose, conectividad hacia Binance y, en la primera construcción, hacia repositorios de imágenes, paquetes Python y Maven. Los comandos se ejecutan desde PowerShell. Los runtimes Python/Java del producto están en imágenes; no hace falta instalar Spark directamente en Windows.

El proyecto debe estar en `D:\bigdata`, con código, Compose, configuración y el registro/modelos de S04. Para un arranque normal no se vuelven a descargar cinco años ni se reentrena. Los volúmenes Docker previos conservan base, Kafka y observabilidad. Para consultar notebooks se levanta Jupyter aparte.

El streaming solicita 3 GiB de heap para su driver; el batch puede solicitar 4 GiB además de memoria de Python/JVM y otros contenedores. Es necesario que Docker disponga de memoria y disco suficientes. Las cifras de hipótesis de costos no son una reserva real de recursos. No se impone aquí un mínimo universal de RAM que no haya sido medido para cada equipo; si hay presión, evitar entrenar y ejecutar el vivo a la vez.

### Arranque habitual de la copia preparada

Abrir Docker Desktop y esperar que el motor responda. Después:

```powershell
Set-Location D:\bigdata
docker version
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\iniciar_producto.ps1
```

El script comprueba Docker y la existencia del registro, crea `.env` desde el ejemplo si falta, construye la imagen base/runtime si no existen y arranca el producto. Aplica migraciones SQL ordenadas y publica metadatos de modelos. Espera hasta 240 segundos a que la salud integral indique ready. Si se supera el plazo, devuelve error con instrucciones de diagnóstico; no anuncia éxito solo porque los contenedores fueron creados.

Puede requerir descarga de JAR en el primer inicio de Spark. La reconciliación inicial prepara contexto sin esperar una hora de mercado. Durante ese tiempo puede haber CALENTANDO o DESACTUALIZADO. Si hay una avería de origen, modelo o servicio, el plazo de cuatro minutos no la oculta ni borra datos.

Para reconstruir imágenes de aplicación cuando se cambia su Dockerfile:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\iniciar_producto.ps1 -Reconstruir
```

Ese parámetro construye el runtime; cambios en el Dockerfile base pueden requerir construir primero `docker compose -f pyspark\compose.yml build`. Un cambio solo de código montado puede requerir recrear/reiniciar el proceso correspondiente, aunque no reconstruir todas las imágenes.

### Accesos y confirmación de funcionamiento

- Grafana: [tablero CryptoPulse](http://localhost:43000/d/cryptopulse-main), usuario `admin`, contraseña local `la contraseña configurada localmente`.
- API completa: [estado](http://localhost:48000/api/status).
- API de señales: [señales](http://localhost:48000/api/signals).
- Prometheus: [alertas técnicas](http://localhost:49090/alerts) y [targets](http://localhost:49090/targets).
- Kafka UI: [inspección de topics](http://localhost:48085).

Verificación integral:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verificar_producto.ps1
```

La verificación S12 exige evidencias previas S03–S06 y repite controles S07–S11: conexión/recepción, archivo, ventanas/checkpoints, inferencia fresca, observabilidad y consultas Grafana. Exige integración PASS de la misma versión de modelo. Un registro viejo de integración no se acepta para una versión nueva. Guarda resultado PASS o FAIL en `_runtime/s12.json`.

La prueba S07 es más estricta sobre el transporte que la política de salud: exige WebSocket conectado y mensajes recientes. Por eso, bajo una caída de WebSocket con recuperación REST aún efectiva, la salud puede conservar datos utilizables y la verificación de S07 fallar indicando que no se confirmó el transporte previsto. Hay que leer el motivo concreto de cada control.

S11 consulta los paneles PostgreSQL mediante la API Grafana y comprueba errores dentro de respuestas HTTP 200. El panel de señales debe devolver una explicación, no cero filas. La evidencia de aceptación anterior comprobó además las siete expresiones Prometheus. La verificación rutinaria del código actual no debe describirse como una captura visual ni como ejecución automatizada de absolutamente todos los ensayos de regresión en cada arranque.

Para una consulta rápida desde PowerShell:

```powershell
$estadoCryptoPulse = Invoke-RestMethod 'http://localhost:48000/api/status'
$estadoCryptoPulse.health
$estadoCryptoPulse.health.symbols
$estadoCryptoPulse.health.prediction_series
$estadoCryptoPulse.signals.state
```

### Jupyter opcional

```powershell
docker compose -f pyspark\compose.yml up -d
```

Abrir [JupyterLab](http://localhost:4488/lab). Si pide token, usar el token configurado localmente en esta configuración. Los notebooks existentes S01/S02 permiten estudiar entorno y transformaciones; el producto persistente usa los módulos Python.

No lanzar varias sesiones Spark de notebook y un entrenamiento simultáneo sin controlar recursos. Spark UI interactiva en `http://localhost:4042` aparece cuando hay una sesión activa y corresponde al contenedor de notebooks.

### Parada conservando información

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\detener_producto.ps1
docker compose -f pyspark\compose.yml stop
```

El primer comando detiene solo los servicios declarados en `platform.yml`; el segundo detiene Jupyter. Se conservan históricos, modelos, checkpoints y volúmenes. El siguiente inicio retoma el trabajo y reconcilia las velas disponibles del período de parada. No hace falta cerrar Docker Desktop para detener el proyecto.

No usar borrado de volúmenes o poda de datos como procedimiento de apagado. La limpieza de caché/imágenes y la eliminación de bases son operaciones distintas que requieren conocer qué se perdería.

### Consultar logs y reiniciar un servicio identificado

```powershell
docker compose -f platform.yml ps
docker compose -f platform.yml logs --tail 100 producer streaming exporter
docker compose -f platform.yml logs --tail 80 kafka postgres prometheus grafana
```

Un reinicio específico, cuando el diagnóstico lo justifique, es:

```powershell
docker compose -f platform.yml restart producer
```

Para recargar un modelo nuevo en el proceso streaming:

```powershell
docker compose -f platform.yml up -d --no-deps --force-recreate streaming
```

Después comprobar la salud y la versión, no asumir que el retorno del comando garantiza siete predicciones nuevas. Recrear con las mismas rutas conserva checkpoints; no proporciona una migración automática si se cambió el formato de estado de las consultas.

## Reproducción del batch y preparación desde una copia sin artefactos

### Qué cambia entre reusar y reconstruir

El repositorio ignora raw, artefactos, `_runtime` y `.env`. Por eso una copia solo de Git puede tener código correcto pero carecer de muestra, millones de velas, modelos y evidencias. `iniciar_producto.ps1` requiere el registro: no pretende entrenar de forma implícita cada vez que se abre Grafana.

Para una copia ya preparada, bastan los comandos de arranque habitual. Para una copia sin artefactos, el orden lógico es construir entorno → descargar → calidad → entrenar/verificar U1 → arrancar plataforma → publicar resúmenes/métricas → pruebas → verificación final. Son procedimientos con costos y duración diferentes.

### Preparar datos y modelos completos

Con los archivos del proyecto disponibles, se puede construir/levantar directamente Jupyter antes del script de entrenamiento. Esto evita que `iniciar_s01.ps1` se detenga si no existe la muestra académica S01, ya que ese script comprueba expresamente su presencia.

```powershell
Set-Location D:\bigdata
docker compose -f pyspark\compose.yml build
docker compose -f pyspark\compose.yml up -d
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\entrenar_producto.ps1 -Descargar -Reprocesar -Reentrenar
```

El script lee el rango de `config/pipeline.json`, descarga los históricos, ejecuta calidad, entrena y verifica la unidad 1. Si el stream del proyecto estaba activo, lo detiene y lo restaura en `finally`. No pausa Docker ni servicios ajenos. Reprocesar fuerza reentrenamiento porque puede cambiar las entradas/modelos. Usarlo sin esos parámetros reutiliza evidencia/artefactos existentes cuando el código del script así lo permite; hay que forzar las etapas si se modificaron sus insumos.

El batch completo puede tardar y utilizar recursos. Su resultado es una versión nueva; las cifras de este informe corresponden a la versión inspeccionada y no deben asignarse automáticamente a una ejecución nueva.

Después:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\iniciar_producto.ps1
docker compose -f platform.yml run --rm --no-deps tools python -m cryptopulse.publish_batch
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\actualizar_costos.ps1
docker compose -f platform.yml run --rm --no-deps tools python -m cryptopulse.verify 6
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\pruebas_integracion.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verificar_producto.ps1
```

El comando completo `publish_batch` hace los resúmenes diarios; el arranque normal llama solo a `--metadata-only`, suficiente para una base ya poblada pero no para reconstruir `batch_kpis` en una base vacía. El control S06 produce evidencia de ida/vuelta Kafka que S12 requiere. La integración prepara su evidencia para la versión recién entrenada. No se sustituye un FAIL copiando un JSON PASS anterior.

`publish_batch` ejecuta Spark para agregar el histórico; si el equipo está justo de memoria, programar esa tarea evitando otras cargas pesadas. El script de entrenamiento pausa el stream automáticamente; la publicación invocada manualmente no tiene ese mismo envoltorio de pausa.

### Descarga histórica sin volver a entrenar todavía

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\preparar_s02.ps1 -Desde 2022-01 -Hasta 2026-09
```

Exige que `cryptopulse-pyspark` esté ejecutándose. Descarga/verifica el origen; no implica haber hecho calidad ni reentrenado. Para ampliar el período, revisar configuración, meses realmente cerrados y cortes temporales del código; cambiar una fecha de descarga no redefine por sí solo validación, test o umbrales.

### Actualización o recalibración de modelos

Un reentrenamiento completo usa `entrenar_producto.ps1 -Reentrenar`. La recalibración implementada se ejecuta con `python -m cryptopulse.training --recalibrate` en el entorno adecuado y reutiliza base/model_dataset; necesita el conjunto preparado y no lo reconstruye si falta. Publica el registro al terminar. No debe ejecutarse como si fuera una consulta liviana mientras otros procesos consumen recursos sin planificación.

Tras cambiar el registro: verificar U1 con `training --verify-u1`, publicar metadatos, recrear el stream para recargarlo, repetir integración para esa versión y verificar S12. La salud detecta desalineación de versión; el usuario debe conservar registros/modelos antiguos que se necesiten para auditar y evaluar predicciones existentes.

No hay reentrenamiento periódico ni automatización de selección de nuevos modelos por rendimiento diario. No se cambia el modelo por haber visto un resultado favorable del test reservado.

## Pruebas, evidencias y alcance de lo comprobado

### Pruebas unitarias y de regresión

La aceptación guardada registra 40 pruebas aprobadas: diez de ingesta/contrato/estado atómico, doce de señales/evaluación, doce de salud/consultas de tablero, tres de contrato de ventanas y tres de separación/publicación de calibración.

```powershell
docker compose -f platform.yml run --rm --no-deps tools python -m unittest cryptopulse.test_ingestion cryptopulse.test_signals cryptopulse.test_health cryptopulse.test_stream_contract cryptopulse.test_training_calibration
```

Comprueban casos concretos: salto WebSocket y recuperación del minuto intermedio; cursor independiente tras reinicio; ACK fallido que no confirma página; solo cierres completos; ausencia en fuente sin fabricación; decimales/tipos inválidos; bloqueo transitorio/permanente de Windows; temporales concurrentes; mercado tranquilo sin falsas alertas artificiales; caída observada distinta de pronóstico; cruces de frontera; actividad por cualquiera de sus medidas; valores ausentes; baseline de divergencia; par antiguo no reutilizado; evaluación con versión propia y rotación de pendientes; datos frescos que no ocultan predicciones antiguas; contexto de 60 filas insuficiente; un activo que no oculta al otro; HTTP 503 si falla inferencia; errores Grafana bajo HTTP 200 y publicación de registro solo al completarse.

Los dobles de prueba y valores sintéticos para verificar reglas no se publican como feed real de mercado. Las pruebas unitarias aisladas no equivalen por sí mismas a comprobar Kafka, Spark y base en ejecución.

### Cinco pruebas con Spark y modelos guardados

```powershell
docker compose -f platform.yml run --rm --no-deps -e CRYPTOPULSE_SPARK_TESTS=1 -e 'PYSPARK_SUBMIT_ARGS=--master local[2] --driver-memory 1g pyspark-shell' tools python -m unittest cryptopulse.test_inference_spark -v
```

Usan velas reales históricas y modelos persistidos, con sinks aislados: siete series y catorce evaluaciones; activo rezagado; retirada/recuperación de un minuto; cinco filas que abarcan seis minutos rechazadas como ventana de cinco; y 61 minutos contrastados con cálculo independiente. La aceptación anterior registró cinco PASS.

### Integración Kafka y reproducción histórica

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\pruebas_integracion.ps1
```

El ensayo crea un topic aislado con identificador único, publica velas reales en un orden controlado y comprueba duplicado contado una vez, descarte tardío por watermark y recuperación desde checkpoint. En la aceptación se observó una fila descartada por watermark y una recuperación correcta. También aplica los cuatro objetivos a replay, conserva siete predicciones, incorpora sus siete resultados futuros y verifica idempotencia.

La prueba de temporalidad cambia precios futuros de una muestra y exige que las variables anteriores no cambien. Eso comprueba una propiedad concreta de las funciones compartidas; no certifica ausencia de todo sesgo estadístico posible. La evidencia contiene la versión de modelo y se exige esa coincidencia en S12. Replays se etiquetan y el tablero principal los excluye.

### Interrupción real controlada

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\prueba_recuperacion.ps1
```

Pausa únicamente el productor durante 75 segundos por defecto (rango permitido 70–120), exige que antes esté listo, comprueba degradación y restaura el productor en `finally`. Espera hasta cuatro minutos a recuperación y comprueba cero huecos recientes y salud de las siete series. **Ejecutarla interrumpe deliberadamente la captura por un tiempo breve**; por eso no es parte obligatoria de abrir el tablero a diario.

La aceptación anterior guardó PASS. Ese ensayo comprueba una interrupción del productor con servicios/datos disponibles; no reproduce la pérdida física de disco, corrupción de PostgreSQL o caída de todo el exchange.

### Evidencias principales y fechas

`_runtime/aceptacion_producto.json` integra resultados: 40 pruebas unitarias, cinco Spark, integración de la versión `20261029T011759858407Z.cal`, interrupción/recuperación y arranque, además de consultas del tablero. En su captura final había 14 señales, estado `no_alerts` y siete series recientes que avanzaron tres minutos entre muestras. Eso confirma progreso en esa ventana de aceptación; no una garantía indefinida.

`s03.json` registra calidad; `s04.json` entrenamiento/recalibración; `s05.json` cierre U1; `s06` a `s12` resultados de sus controles. `integration_tests.json`, `recovery_acceptance.json`, `ingestion_repair.json`, `startup_acceptance.log`, `recalibration.log` y `verification_product_latest.log` permiten revisar las ejecuciones. Los logs pueden contener advertencias no fatales; el resultado final y su error concreto determinan si el paso terminó.

La inspección visual automatizada del tablero compartido no estuvo disponible durante la aceptación anterior por una discrepancia de sesión de navegador. Se verificaron datos y consultas mediante API; no se afirma que haya una certificación visual completa de pantalla. Durante esta elaboración documental no se reejecutaron entrenamientos, ensayos destructivos ni la interrupción de 75 segundos, ya que el motor Docker estaba apagado y esas acciones no eran necesarias para describir lo construido.

## Diagnóstico de problemas frecuentes

### Docker no responde

`docker version` debe mostrar Client y Server. Si falta Server o no existe `dockerDesktopLinuxEngine`, abrir Docker Desktop y esperar el motor Linux. Un error `permission denied` dentro de una herramienta con sandbox puede ser una limitación de esa sesión, aun cuando PowerShell del usuario sí tenga acceso. Es distinto de motor apagado. No se arregla borrando volúmenes del proyecto.

### Grafana conserva un precio, pero el producto está degradado

Revisar edades por activo y siete series, heartbeats y versión en `/api/status`. Grafana puede dibujar un dato guardado aunque no haya llegado uno nuevo. El primer precio o un HTTP 200 de `/api/status` no sustituyen `health.ready`.

### Centro de señales vacío o historial sin eventos

Si falta `active_model_version` o las migraciones/vistas, el tablero puede carecer de filas: aplicar arranque/migraciones/publicación y revisar logs SQL. Con catorce NORMAL no hay un error por ausencia de alarmas. Con SIN EVALUAR/DESACTUALIZADO revisar contexto o ingesta. Un historial SIN CRUCES en el período elegido es una respuesta válida; no se modifica un umbral para llenarlo.

### Hay una vela intermedia ausente

Esperar reconciliación reciente y consultar `source_gaps`. Si persiste o está fuera del solapamiento, ejecutar la herramienta de reparación ya descrita y verificar el sink. Si el proveedor tampoco devuelve esa vela, conservar el hueco y su diagnóstico. La cobertura de ventanas antiguas puede seguir incompleta por watermark, aunque el archivo se repare.

### Las predicciones no avanzan pese a recibir precios

Comprobar `INFERENCE_ENABLED`, versión del registro y del stream, continuidad de 61 velas, sincronización del par y logs de inferencia. Una entrada del modelo nula excluye esa fila. Una correlación indefinida puede impedir la serie conjunta. Recuperar datos permite volver a calcular; rellenar nulos arbitrariamente cambiaría el significado analítico.

### Muchas evaluaciones pendientes

Separar horizontes todavía abiertos de los ya maduros. Consultar `evaluation_status/reason`, ausencia de velas y disponibilidad de la versión original. La evaluación ocurre en lotes válidos del archivo; si se detiene el stream, no hay proceso independiente que complete continuamente todas las pendientes. El indicador de backlog maduro permite detectarlo.

### Cambiar `.env` no cambia un acceso existente de Grafana/PostgreSQL

Las variables de creación no necesariamente sobrescriben usuarios ya guardados en un volumen. Grafana conserva su base y PostgreSQL inicializa usuarios al crear el directorio por primera vez. Si se cambia una credencial, actualizar el usuario persistido y la configuración de clientes de forma coherente, sin borrar la base para forzar un cambio. La contraseña `la contraseña configurada localmente` solicitada ya fue aplicada al usuario persistido en el trabajo anterior.

### Fechas parecen distintas entre archivos y navegador

Los tiempos de mercado son UTC; la versión `.cal` lleva fecha UTC y algunos registros pueden conservar un nombre de campo UTC con texto desplazado por la herramienta que los generó. Revisar el offset explícito. Lima es UTC−5: la versión del 29 de octubre 01:17 UTC corresponde al 28 de octubre 20:17 en Lima. Grafana muestra zona del navegador salvo que la URL/tablero la sobreescriba.

## Persistencia, tamaño de carpeta y copias

### Por qué ocupa espacio

En la medición previa, `D:\bigdata` ocupaba aproximadamente 1,52 GiB. La mayor parte eran datos y derivados: 114 CSV alrededor de 706 MiB y sus 114 ZIP alrededor de 211 MiB; Parquet limpio cerca de 410 MiB; caché Ivy cerca de 120 MiB; artefactos de modelado alrededor de 72 MiB. Las cifras pueden variar al reentrenar o guardar más evidencias.

ZIP permite conservar el paquete verificado; CSV es el origen extraído; Parquet es la representación analítica tipada; model_dataset/pair_dataset permiten reproducir aprendizaje; modelos y registros conservan el resultado; checkpoints conservan continuidad de procesamiento; JAR/imágenes evitan descargar dependencias de nuevo. Cada categoría tiene una función.

Docker ocupa espacio adicional fuera de esa carpeta. La medición anterior mostraba imágenes 4,959 GB, build cache 1,932 GB y volúmenes alrededor de 414,9 MB; esos informes pueden compartir capas y no deben sumarse como si fueran todos bloques exclusivos. Son mediciones de aquella fecha, no el tamaño actual garantizado.

Para medir cuando Docker esté disponible:

```powershell
docker system df
docker compose -f platform.yml ps
```

Eliminar CSV, Parquet, modelos, checkpoints o volúmenes tiene consecuencias distintas. Una entrega de operación reducida podría excluir algunos insumos de entrenamiento si se conserva una copia reproducible aparte, pero la copia actual mantiene el material completo. Este informe no realizó limpieza.

### Qué conservar para recuperar el proyecto

Una copia completa necesita código/configuración, históricos/manifiestos si se quiere reproducir batch, artefactos/modelos/registros, estado/checkpoints del proceso y respaldo de las bases/volúmenes pertinentes. Guardar solo un `.ipynb` o solo el JSON de Grafana no reconstruye todo el sistema.

Un dump PostgreSQL contiene tablas, señales y predicciones. Los modelos Spark están en archivos y deben copiarse por separado. Kafka guarda el log en su volumen; los checkpoints deben ser compatibles con el log conservado. Grafana guarda usuarios/configuración en su volumen aunque el dashboard/datasources estén provisionados desde archivos. Prometheus guarda su historia operativa en el suyo.

### Ejemplo de copia lógica PostgreSQL sin corromper binario en PowerShell

Los comandos siguientes son una guía para ejecutar cuando se desea una copia; no se ejecutaron al redactar este informe. El dump se genera dentro del contenedor y se copia como archivo, evitando redirigir bytes binarios por versiones de PowerShell que pudieran transformarlos.

```powershell
Set-Location D:\bigdata
$fechaCopiaCryptoPulse = Get-Date -Format 'yyyyMMdd-HHmmss'
$destinoCopiaCryptoPulse = Join-Path 'D:\bigdata\backups' $fechaCopiaCryptoPulse
New-Item -ItemType Directory -Path $destinoCopiaCryptoPulse -Force | Out-Null
docker compose -f platform.yml exec -T postgres pg_dump -U cryptopulse -d cryptopulse -Fc -f /tmp/cryptopulse-backup.dump
if ($LASTEXITCODE -ne 0) { throw 'Fallo pg_dump; no considerar valida la copia.' }
docker cp cryptopulse-postgres:/tmp/cryptopulse-backup.dump (Join-Path $destinoCopiaCryptoPulse 'cryptopulse.dump')
if ($LASTEXITCODE -ne 0) { throw 'Fallo la copia del dump.' }
Copy-Item -LiteralPath '.\platform.yml' -Destination $destinoCopiaCryptoPulse
```

El proyecto no incluye un plan automático de backups/restauración probado. El ejemplo no copia todavía los modelos ni volúmenes Kafka/Grafana/Prometheus: hay que añadirlos al plan de respaldo y registrar versión, hora y rutas. No almacenar una copia recursiva completa dentro de la misma carpeta que se está copiando. Para consistencia de un conjunto con checkpoints/log, programar una parada controlada o una estrategia coordinada; el pipeline no dispone de una instantánea transaccional global de todos sus componentes.

Antes de sustituir una base, verificar el dump con `pg_restore --list` y ensayar recuperación en un entorno separado. La recuperación completa debe comprobar claves, versión activa, modelos disponibles, permisos de lectura, series y continuidad. Una copia que no se ha restaurado en prueba no demuestra por sí sola que pueda recuperarse.
