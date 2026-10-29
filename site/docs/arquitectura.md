# Arquitectura Lambda y despliegue

![Diagrama del sistema](assets/images/arquitectura.svg)

*Diagrama técnico; las conexiones están implementadas en los archivos Compose y módulos enlazados.*
## 3. Arquitectura implementada y recorrido completo

### 3.1 Por qué se considera una arquitectura Lambda

Lambda separa el trabajo histórico de la actualización de baja latencia y reúne sus resultados en una capa de consulta. En CryptoPulse la capa batch prepara millones de velas, entrena y valida modelos. La capa de velocidad incorpora velas recién cerradas, calcula variables y aplica los modelos ya guardados. La capa de servicio es PostgreSQL con tablas/vistas consultadas por Grafana, más una API de estado y señales.

La convergencia ocurre de dos maneras: los modelos y umbrales del batch se usan en la inferencia viva; los resúmenes históricos y resultados recientes se publican en la misma base de servicio. Las funciones de variables se comparten para reducir diferencias entre entrenamiento e inferencia.

La arquitectura no es Kappa pura: no se ha construido todo el histórico exclusivamente reproduciendo un único log Kafka. El batch descarga archivos mensuales y tiene su propio camino de preparación. Tampoco es una instalación de alta disponibilidad: hay contenedores separados, un solo broker y un motor Spark local en un equipo.

*Diagrama del flujo representado en la imagen de arquitectura de esta página.*

Si el lector Markdown no representa Mermaid, cada flecha equivale a una transferencia de datos o una consulta entre los componentes nombrados; el texto de los capítulos desarrolla el mismo recorrido.

### 3.2 Ejemplo temporal de una actualización

Supóngase una vela que abre a las 10:00:00 UTC y termina a las 10:00:59.999. Mientras está abierta, Binance puede emitir varias actualizaciones; CryptoPulse espera `k.x=true`. Ese cierre se convierte en el ancla analítica `as_of_ms`.

El productor valida el evento y espera confirmación de Kafka. En un micro-lote posterior, el archivo lo inserta en PostgreSQL. La inferencia consulta hasta 180 velas recientes por activo; si las variables necesarias están completas, produce las siete salidas del ancla disponible y sus señales. Grafana las consulta al refrescar. El procesamiento no espera 15 minutos para emitir la primera predicción de volatilidad: emite una estimación ahora y espera ese horizonte para conocer su verdad futura.

Para ese cierre de ejemplo, la actividad futura abarca los cinco cierres siguientes y termina a las 10:05:59.999; la volatilidad y divergencia terminan a las 10:15:59.999; la caída se evalúa con el cierre de las 10:30:59.999. Las tablas conservan el origen temporal de la estimación. La verdad futura aparece después y se alinea en los gráficos al ancla del pronóstico, no al instante en que terminó de evaluarse.

Las ventanas fijas de streaming siguen otra lógica: acumulan eventos por intervalos de calendario y se publican cuando avanza el watermark. No son el mecanismo que bloquea o dispara los horizontes de los modelos.

### 3.3 Qué aporta la plataforma más allá de dibujar precios

El precio aislado no informa si la actividad es extraordinaria para su horario, si la variación actual es extrema en la escala del activo o si SOL se separa de BTC en términos relativos. CryptoPulse añade esas comparaciones, anticipaciones evaluadas y una comprobación técnica de que los datos permiten interpretarlas.

Su utilidad tiene tres niveles: seguimiento descriptivo de hechos, vigilancia predictiva con errores visibles y trazabilidad operativa del proceso. La calidad del pipeline y la capacidad predictiva son dimensiones diferentes: un sistema puede estar técnicamente listo y tener un modelo con baja precisión. El informe y el tablero mantienen esa diferencia.

## 5. Despliegue físico, puertos y datos persistentes

### 5.1 Servicios y conexiones

| Servicio | Proceso o función | Dirección desde Windows | Dirección entre servicios |
|---|---|---|---|
| `kafka` | Broker/controlador KRaft | `localhost:49092` | `kafka:9092`; controlador interno 9093 |
| `postgres` | Base CryptoPulse | Sin puerto publicado | `postgres:5432` |
| `producer` | Captura WS y reconciliación REST | Sin interfaz publicada | Kafka y salidas HTTPS/WSS |
| `streaming` | Driver Spark y tres consultas | Sin puerto de UI publicado | Kafka y PostgreSQL |
| `exporter` | API y métricas | `http://localhost:48000` | `exporter:8000` |
| `kafka-exporter` | Métricas del broker | Sin puerto publicado | `kafka-exporter:9308` |
| `kafka-ui` | Inspección Kafka | `http://localhost:48085` | `kafka-ui:8080` |
| `prometheus` | Series técnicas y reglas | `http://localhost:49090` | `prometheus:9090` |
| `grafana` | Dashboard | `http://localhost:43000` | `grafana:3000` |
| `tools` | Tareas efímeras, perfil tools | Sin interfaz publicada | Misma red del producto |
| `pyspark` | Jupyter/Spark interactivo separado | `http://localhost:4488`; Spark UI 4042 si existe una sesión activa | Proyecto Compose independiente |

Los nueve servicios persistentes del producto comparten `cryptopulse-net`. Jupyter tiene su configuración Compose separada y monta la misma carpeta de sesiones para acceder a históricos y modelos; no es el consumidor Kafka del producto. Su interfaz Spark UI en 4042 corresponde al Spark interactivo, no necesariamente al driver persistente de streaming.

Los nombres de host internos solo se resuelven en la red Docker. En un navegador Windows se usa `localhost` y el puerto publicado; desde un contenedor, `localhost` se refiere al propio contenedor, por lo que Grafana consulta `postgres:5432` y `prometheus:9090`.

### 5.2 Imágenes, montajes y volúmenes

La imagen base `cryptopulse-pyspark-pyspark:latest` se construye con `pyspark/Dockerfile`. `cryptopulse-runtime:local` parte de esa imagen y añade clientes Kafka y PostgreSQL. Productor, streaming, exportador y tools reutilizan ese runtime, cada uno con un comando distinto.

La carpeta `D:\bigdata\pyspark\sesiones` se monta como `/opt` en los procesos Python. Allí están el código, datos, modelos y `_runtime`. Un cambio del código montado se ve en disco dentro del contenedor, pero un proceso que ya importó módulos o cargó un modelo puede necesitar reinicio para utilizarlo. El registro de modelos se lee al crear `ModelBundle`.

Los volúmenes nombrados del producto son `cryptopulse_kafka_data`, `cryptopulse_postgres_data`, `cryptopulse_prometheus_data` y `cryptopulse_grafana_data`. Guardan respectivamente log Kafka, base PostgreSQL, series técnicas y configuración/usuarios de Grafana. Docker puede mostrar además volúmenes anónimos derivados de imágenes. No todos esos datos son archivos ordinarios visibles bajo `D:\bigdata`.

Detener contenedores conserva sus volúmenes y los bind mounts. Copiar únicamente la carpeta del proyecto conserva fuentes y artefactos montados, pero no equivale a una copia de PostgreSQL ni de los otros volúmenes. El procedimiento de copia se explica en operación.

### 5.3 Configuración y parámetros de entorno

`config/pipeline.json` define `history_from=2022-01` y `history_to=2026-09`. `config/cost-assumptions.json` contiene las hipótesis del escenario económico. `.env` permite a Compose suministrar `INFERENCE_ENABLED`, contraseña local de PostgreSQL, acceso Grafana y token de Jupyter cuando corresponde. No se precisan tokens Binance para las fuentes públicas de este producto.

Las aplicaciones reciben `PYTHONPATH=/opt`, `KAFKA_BOOTSTRAP=kafka:9092`, `PGHOST=postgres`, `PGDATABASE=cryptopulse`, `PGUSER=cryptopulse` y `PGPASSWORD` desde Compose. `CRYPTOPULSE_ROOT` permite sustituir `/opt` en las utilidades compartidas; el despliegue descrito usa `/opt`. La inferencia solo se activa cuando `INFERENCE_ENABLED` es `true` en el servicio streaming; la salud integral informa si está deshabilitada.

Los accesos de uso solicitados por el usuario son Grafana con usuario `admin`, contraseña `la contraseña configurada localmente`, y Jupyter con token local el token configurado localmente. No se necesita una clave de cuenta para abrir Prometheus o Kafka UI en esta configuración local. Es una configuración de equipo local; publicar esos puertos a una red requeriría revisar autenticación y acceso.


## Código de referencia

[platform.yml](https://github.com/abdul8377/big-data-btc-sol/blob/main/platform.yml) · [pyspark/compose.yml](https://github.com/abdul8377/big-data-btc-sol/blob/main/pyspark/compose.yml) · [database/init.sql](https://github.com/abdul8377/big-data-btc-sol/blob/main/database/init.sql)
