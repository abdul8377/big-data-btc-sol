# Bitácora funcional S01–S12

## Qué se hizo en cada sesión S01–S12

La guía y repositorio `lambda26` se utilizaron como base técnica. CryptoPulse adapta sus capacidades al brief de mercado: en lugar de copiar los sensores, dominios y datasets de ejemplo, conserva las etapas necesarias para BTC/SOL. Los roles, firmas, capturas académicas y asignaciones de integrantes se dejaron fuera por solicitud del usuario.

### S01 — Arquitectura y entorno

Se eligió Lambda y se preparó Docker, Python, Java, Spark y Jupyter. Se descargó una muestra diaria real de BTC/SOL del 1 de enero de 2025, con checksums y 2.880 velas. El notebook verifica lectura/esquema, sesión Spark y cálculos iniciales. El entorno se trasladó de la carpeta inicial de Documents/Codex a `D:\bigdata`, donde se mantiene la copia activa.

Su aportación al producto es un entorno reutilizable y una primera prueba de que se leen datos reales. La muestra no se utiliza como sustituto del histórico completo. `iniciar_s01.ps1`, descargador y notebook documentan ese arranque; el token de Jupyter configurado es el token configurado localmente.

### S02 — Fundamentos y transformaciones PySpark

Se añadieron descarga mensual parametrizada, manifiesto, esquema explícito, conversión temporal y comparación entre inferencia de tipos y tipos controlados. La práctica comenzó con diciembre 2024/enero 2025 de ambos activos, 178.560 velas, para comprobar también el cambio milisegundos/microsegundos.

El notebook trabaja selecciones, filtros, columnas de calendario/dirección, rendimientos, agregaciones diarias, OHLC horario ordenado y VWAP. Este último es importe negociado dividido por volumen base cuando el denominador es válido, distinto de promediar cierres. Escribe/relee CSV y Parquet y contrasta operaciones RDD (`map`, `filter`, `flatMap`, `reduceByKey`) con DataFrames. Demuestra lazy evaluation mediante planes y acciones.

Se corrigieron divisiones Decimal en rendimientos/logaritmos y ventanas sin calentamiento completo. El notebook ejecutado conserva salidas sin errores según revisión anterior; el JSON individual suelto S02 no estaba presente entonces. No se crea un PASS documental para reemplazar una evidencia ausente. El manifiesto compartido se amplió luego a 2022–2026.

### S03 — Calidad y almacenamiento analítico

Se prepararon 114 archivos y 4.993.759 entradas; quedaron 4.993.757 limpias. Dos intervalos inválidos fueron a cuarentena. Se comprobaron campos, OHLC, actividad, tiempos, duplicados/conflictos y tres discontinuidades con 163 minutos-activo ausentes. Se escribió Parquet particionado y se verificó partition pruning. Es la entrada controlada del aprendizaje.

### S04 — Aprendizaje distribuido y evaluación temporal

Se construyeron variables retrospectivas y cuatro objetivos, entrenamiento 2022–2024, validación 2025 y test 2026 hasta septiembre. Se aplicaron embargos, muestreo cada quince minutos, modelos/registros persistidos, calibración y referencias. Se evaluaron Random Forest/ridge/logísticas; divergencia mantuvo referencia cero. La implementación funcional reside en módulos compartidos y artefactos, no en cuatro notebooks de dimensión que deban suponerse existentes.

La revisión posterior corrigió el embargo entre calibración y selección y generó la versión `.cal` vigente. Las métricas de este informe corresponden a esa revisión, no a la primera entrega sin corrección.

### S05 — Cierre funcional de unidad 1

`training --verify-u1` confirma calidad PASS con más de un millón de filas, existencia de metadatos de modelos, muestras no vacías en particiones y resultados test de cuatro dimensiones. Guarda `s05.json`. Verifica el conjunto batch preparado y evaluado; no implica que ya esté conectado el feed de mercado.

### S06 — Eventos y Kafka

Se creó broker KRaft, topics, contrato v1 y una prueba de publicación/lectura con identificador de sonda. Verifica la posición devuelta por Kafka y que el consumidor recupera ese mensaje. `verify 6` guarda partición/offset y evidencia. El evento de sonda va a integración, separado de las velas analíticas.

### S07 — Captura de mayor frecuencia

Se conectó el WebSocket público de BTC/SOL y recuperación REST. Se validan cierres reales, procedencia y cadencia. La verificación exige mensajes recientes y lectura de eventos válidos en ambos topics. Sustituye la fuente IoT de ejemplo por telemetría real del dominio del brief. Posteriormente se corrigió el cursor REST para recuperar minutos intermedios perdidos.

### S08 — Procesamiento con estado

Se implementaron archivo, ventanas fijas/deslizantes, deduplicación acotada, watermark, sinks PostgreSQL y checkpoints. La evidencia comprueba archivos/estados de checkpoint y resultados de ambos tipos de ventanas. Se explicita entrega al menos una vez con claves idempotentes. Las reparaciones tardías del archivo no equivalen a reabrir ventanas purgadas.

### S09 — Observabilidad y costos

Se añadieron exportador/API, Prometheus, Kafka Exporter, reglas y escenario de costos. La configuración inicial tenía cinco reglas; las correcciones incorporaron predicción atrasada, huecos y salud integral, sumando ocho. El control comprueba targets UP y configuración de costos ilustrativa, sin afirmar que sea facturación real.

### S10 — Inferencia en streaming

Se cargan pipelines versionados y se aplican después de velas archivadas, con las mismas variables del batch. Se producen siete series y se incorporan resultados futuros al madurar. Se añadieron continuidad rigurosa, sincronización actual de par, registro de versiones y rotación/causas de pendientes. La verificación exige salud fresca, no solo existencia de predicciones de días anteriores.

### S11 — BI y ML en un tablero

Se publicaron KPI batch y métricas históricas a PostgreSQL, fuentes de lectura y dashboard provisionado. La revisión amplió el diseño al estado actual: 23 paneles de datos, una guía y cuatro encabezados; centro de catorce señales y fallback explicativo del historial. SQL y Prometheus convergen en un solo Grafana, con estados y unidades diferenciados.

### S12 — Cierre funcional de unidad 2

Se integraron validaciones S03–S11 con prueba de duplicados, frescura, cuatro dimensiones, integración de la versión actual y evaluación futura. El cierre anterior fue PASS y quedó guardado. Las pruebas de recuperación, temporalidad e idempotencia respaldan propiedades concretas; no convierten el despliegue local en alta disponibilidad ni garantizan que cualquier modelo sea predictivamente útil.

Las sesiones S13–S16 no forman parte del alcance solicitado hasta S12. Algunas mejoras de integración ya se realizaron para cerrar errores, pero no se presenta como si el curso entero o una certificación académica se hubiera completado.
