# Herramientas: para qué se usa cada una

## 4. Herramientas y responsabilidad concreta de cada una

### Docker Desktop y Docker Compose

Docker Desktop ejecuta contenedores Linux en Windows. Un contenedor encapsula el proceso y su entorno; una imagen contiene las dependencias necesarias para crearlo. Compose declara servicios, red, puertos, volúmenes, comandos y condiciones de arranque. `platform.yml` es la declaración del producto persistente; `pyspark/compose.yml` declara el entorno interactivo independiente.

Esto permite ejecutar el mismo Python y Java aunque Windows tenga otras versiones instaladas. Los puertos publicados se ligan a `127.0.0.1`, por lo que los accesos descritos son locales. Tener varios contenedores permite separar responsabilidades; no multiplica la memoria física ni convierte un único ordenador en un clúster de varios servidores.

Los servicios persistentes tienen `restart: unless-stopped`: pueden reiniciar tras un fallo mientras Docker esté disponible, pero una parada deliberada permanece hasta volver a arrancarlos. `tools` es una ejecución temporal para tareas de mantenimiento/verificación y no un servicio que deba quedar encendido.

### Python 3.10, Java 21 y PySpark 4.2.0

Python organiza descarga, validación, publicación, inferencia y comprobaciones. Java 21 proporciona la JVM que ejecuta Spark. PySpark es la interfaz Python del motor Spark; sus DataFrames expresan operaciones que Spark planifica y ejecuta. Una transformación como `filter` define un plan; acciones como `count`, `collect` o escritura lo ejecutan. El entorno conserva esa distinción desde S02.

El batch usa por defecto `local[4]`, memoria de driver `4g`, 16 particiones de shuffle, zona UTC y ejecución adaptativa. El streaming se lanza con `local[2]`, driver `3g` y cuatro particiones de shuffle. Esas cifras son configuraciones del proceso, no medidas de consumo permanente ni número de máquinas. El batch puede competir por recursos con el vivo; por eso el script de entrenamiento pausa el stream del proyecto y lo restaura al salir.

### Spark SQL y Structured Streaming

Spark SQL ejecuta filtros, joins, expresiones, agregaciones y funciones de ventana sobre tablas lógicas. Structured Streaming aplica ese motor a datos que crecen. En este proyecto utiliza micro-lotes cada 15 segundos, Kafka como entrada y `foreachBatch` como salida a PostgreSQL. Sus checkpoints conservan offsets y estado; las claves SQL hacen que una repetición no duplique resultados por negocio.

El conector Kafka cargado es `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`; el sufijo `_2.13` identifica compatibilidad binaria de Scala. Los JAR se resuelven mediante Ivy y se cachean en `_runtime/ivy`; no se usa ese directorio como almacenamiento de precios.

### Spark MLlib y sus pipelines

MLlib entrena los estimadores y guarda pipelines con ensamblado de variables y, cuando corresponde, escalado. `PipelineModel.load` permite aplicar esas mismas transformaciones en vivo. El entrenamiento es batch; `.transform()` en streaming aplica un modelo existente. No hay actualización incremental de pesos con cada vela.

Se entrenaron regresión lineal ridge, Random Forest y regresiones logísticas, y se compararon con referencias simples. La selección concreta y la calibración se detallan más adelante. No se usan APIs de IA, claves de OpenAI ni modelos de lenguaje para producir estos indicadores.

### Apache Kafka 4.3.1 y el cliente kafka-python 3.0.12

Kafka conserva eventos ordenados por partición en disco y desacopla el productor del consumidor. Si Spark se retrasa, puede continuar desde sus offsets mientras Kafka conserve esos datos. El cliente Python crea topics, publica eventos, espera confirmaciones y consulta offsets para diagnóstico.

El broker usa KRaft y combina roles `broker,controller`; no hay ZooKeeper. Hay un único nodo, factor de replicación 1 y retención configurada de 168 horas. Por ello, una copia perdida del volumen Kafka no tiene una segunda réplica local que la reemplace. El productor usa idempotencia y `acks=all`, lo que protege frente a determinados reintentos, pero con un solo broker «all» no significa varias copias independientes.

### PostgreSQL 17.6 y psycopg2-binary 2.9.13

PostgreSQL es la base de servicio: conserva velas recientes/recuperadas, predicciones originales, resultados posteriores, señales, ventanas, resúmenes batch y registros de modelos. Sus claves primarias resuelven idempotencia por identidad de negocio. `psycopg2` es el controlador Python; usa parámetros SQL para valores y `execute_values` para inserciones en lote. Cada bloque de conexión confirma o revierte su transacción.

El usuario de Grafana, `grafana_reader`, tiene permisos de lectura. El escritor del pipeline utiliza el usuario de la base configurado en Compose. PostgreSQL no publica un puerto al host en `platform.yml`: se consulta mediante red interna, Docker exec o Grafana. No se utiliza un data warehouse cloud ni una réplica CDC.

### Grafana 11.4.0

Grafana consulta PostgreSQL para datos analíticos y Prometheus para métricas técnicas. Presenta ambas fuentes en un único tablero; no entrena modelos ni captura el feed de Binance. El JSON se genera en `scripts/generar_dashboard.py` y se monta con provisión automática. La definición se revisa cada 10 segundos; el refresco visual predeterminado es también 10 segundos, pero son operaciones diferentes.

El tablero conserva UID `cryptopulse-main`, carpeta CryptoPulse, intervalo inicial de tres horas y zona horaria del navegador. Una URL antigua puede imponer UTC u otro intervalo. Cambiar el reloj mostrado no cambia la hora UTC usada para umbrales estacionales.

### Prometheus 3.14.0 y prometheus_client

Prometheus recoge y guarda series de salud operativa. Consulta `/metrics` del exportador cada 15 segundos y evalúa reglas cada 15 segundos. La biblioteca Python `prometheus_client` convierte las mediciones del proyecto a su formato de exposición. Esas mediciones responden si llegan datos, si Spark avanza y si las predicciones son recientes; no sustituyen las señales financieras almacenadas en PostgreSQL.

### Kafka Exporter 1.10.0 y Kafbat Kafka UI 1.5.0

Kafka Exporter consulta el broker y expone métricas de topics, particiones y offsets a Prometheus. El tablero usa el offset actual por partición agregado por topic para observar avance. El lag principal de CryptoPulse se calcula por separado comparando broker y checkpoint de Spark, porque los offsets de Structured Streaming no deben confundirse con el commit de un consumidor convencional.

Kafka UI permite inspeccionar topics, particiones y mensajes desde el navegador. Es una herramienta de diagnóstico; el recorrido de producción no necesita que una persona abra su interfaz. Tampoco es el productor del mercado.

### Jupyter Notebook 7.6.2, nbclient 0.10.2 y pandas 2.3.3

Jupyter ofrece el entorno de exploración y los notebooks iniciales del curso. `nbclient` permite ejecutar notebooks de forma automatizada y conservar salidas. pandas facilita operaciones tabulares auxiliares en ese entorno; la transformación y el aprendizaje principales usan Spark. Jupyter no es el servidor del pipeline vivo y puede detenerse sin cerrar Grafana ni el streaming.

Los módulos funcionales del producto están en `pyspark/sesiones/cryptopulse`. Las sesiones posteriores se implementaron en esos módulos y scripts compartidos; no debe suponerse que existen doce notebooks o doce carpetas completas de producto por tener doce sesiones cubiertas.

### Formatos y bibliotecas auxiliares

CSV conserva el intercambio de origen; ZIP comprime la descarga; SHA256 compara integridad con el checksum oficial; JSON guarda configuración y evidencias; JSONB permite consultar estructuras dentro de PostgreSQL; Parquet almacena columnas tipadas para lectura analítica; los modelos Spark incluyen metadatos y archivos propios de sus estimadores. Son representaciones con propósitos diferentes, no copias accidentalmente idénticas.

`requests` realiza llamadas HTTP REST, `websocket-client` recibe el feed público y las bibliotecas estándar manejan fechas, decimales, hashes, JSON y archivos. Los Dockerfiles fijan las versiones principales indicadas arriba; varias bibliotecas auxiliares llegan como dependencias transitivas y no tienen un lockfile completo de entorno. Conviene registrar un inventario de paquetes si se necesita reproducir exactamente todas sus versiones en otra fecha.

NumPy se utiliza de forma concreta en la calibración y selección: convierte probabilidades/etiquetas en arrays, calcula cuantiles y resuelve el pequeño sistema lineal del ajuste tipo Platt. Ese ajuste recoge las puntuaciones del tramo de validación en el driver; no es un entrenamiento completamente distribuido de cada operación numérica. También es una razón para no extrapolar el diseño local a un conjunto de calibración ilimitado sin revisar memoria.

PowerShell organiza arranque, entrenamiento, pruebas y parada desde Windows. Git conserva el código; `.gitignore` excluye `.env`, datos raw, artefactos y `_runtime`. Clonar solo el repositorio no incluye automáticamente históricos, modelos o checkpoints excluidos.
