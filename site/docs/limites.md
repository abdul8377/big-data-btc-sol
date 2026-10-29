# Alcance, limitaciones y mantenimiento

## Limitaciones y correspondencia honesta con el brief

El producto implementa ingesta real batch/viva, calidad, cuatro objetivos, modelos evaluados contra referencias, inferencia con Kafka/Spark, resultados posteriores y un Grafana común. La actividad tiene Z-score como KPI adicional y percentiles como regla principal. Los alcances de compra/venta, atribución causal, fraude, otros exchanges/futuros y disponibilidad garantizada siguen excluidos conforme al brief.

Hay varias diferencias y límites que importan para evaluar el resultado:

1. **Divergencia:** se entrenó un regresor, pero no superó la referencia; el producto mantiene observación útil y pronóstico cero identificado. No se ha demostrado ventaja predictiva de ML en las cuatro dimensiones.
2. **Caídas:** mejoró Brier ligeramente, pero precisión y recall siguen bajos. La mejora de pipeline no aumenta automáticamente esa capacidad predictiva.
3. **Frecuencias:** el histórico supervisado usa anclas cada quince minutos; la producción cada minuto, con objetivos que se solapan y dependencia temporal. La evaluación viva se presenta aparte.
4. **Una sola fuente:** el resultado depende de Binance Spot y de su disponibilidad/calidad. No valida contra cotizaciones de varios proveedores.
5. **Ventanas/archivo:** puede haber ventanas finalizadas incompletas; la reparación de archivo no las reconstruye automáticamente.
6. **Retención:** Kafka conserva siete días según configuración; tablas SQL y evidencias no tienen una política automática de purga completa y pueden crecer.
7. **Escalado:** driver local, `collect()` y sinks desde un proceso limitan capacidad; un broker no replica. No se ha realizado un benchmark de carga masiva ni un ensayo de alta disponibilidad.
8. **Modelos:** registro local, sin MLflow, detector formal de drift, reentrenamiento automático, intervalos predictivos, validación walk-forward completa o optimización económica de alertas.
9. **Alertas:** reglas visibles, sin distribución externa; no hay workflow de reconocer/cerrar incidentes de mercado ni notificación garantizada.
10. **Reproducibilidad de entorno:** versiones principales fijadas, dependencias transitivas sin lockfile exhaustivo; la muestra/artefactos no viajan necesariamente con Git.
11. **Contrato:** JSON Schema y validación efectiva no son idénticos; el esquema declarativo no se impone por un Schema Registry. La procedencia declarada no equivale a una firma del proveedor.
12. **Entrega académica:** se priorizó la parte funcional autorizada. No se completan nombres/roles, firmas o capturas ni se afirma cumplimiento formal de una evaluación docente. S04 y etapas posteriores se concentran en módulos, sin prometer notebooks separados por cada dimensión.

Como mejoras futuras razonables: estudiar por activo el clasificador de caídas y su calibración/umbrales con costos de error explícitos; validar con períodos móviles sin tocar el test reservado; entrenar alternativas de divergencia y desplegarlas solo si vencen su baseline; formalizar lockfile y backups; alinear completamente contrato declarativo/ejecución; añadir retención y reconstrucción de ventanas; mejorar medición de latencia y backlog; incorporar canales de avisos si el usuario lo pide. Son propuestas, no prestaciones ya presentes en el producto.

## Inventario de archivos y responsabilidad para mantenimiento

| Ruta desde `D:\bigdata` | Responsabilidad |
|---|---|
| `platform.yml` | Servicios persistentes, red, montajes, imágenes y puertos. |
| `pyspark/Dockerfile`, `pyspark/compose.yml` | Imagen base y entorno interactivo. |
| `runtime/Dockerfile` | Clientes para ejecutar productor, stream, API y tools. |
| `config/pipeline.json` | Corte de descarga histórica. |
| `config/cost-assumptions.json` | Hipótesis de costos. |
| `schemas/market-kline-v1.schema.json` | Contrato declarativo de eventos. |
| `database/init.sql` | Esquema inicial y usuario de lectura. |
| `database/migrations/002_product_signals.sql` | Señales, registros, configuración y evaluación. |
| `database/migrations/003_dashboard_views.sql` | Índices y vistas de producto. |
| `observability/prometheus.yml`, `alerts.yml` | Targets, intervalos y ocho reglas técnicas. |
| `observability/grafana/provisioning` | Fuentes y provisión de carpeta/tablero. |
| `observability/grafana/dashboards/cryptopulse.json` | Dashboard que Grafana carga. |
| `scripts/generar_dashboard.py` | Fuente de la definición del dashboard; regenerar y publicar JSON tras cambios. |
| `scripts/iniciar_producto.ps1`, `detener_producto.ps1` | Arranque con migraciones/espera y parada conservando datos. |
| `scripts/entrenar_producto.ps1` | Descarga/calidad/modelos y pausa/restauración del stream. |
| `scripts/verificar_producto.ps1`, `pruebas_integracion.ps1` | Verificación y pruebas con Kafka/Spark. |
| `scripts/prueba_recuperacion.ps1` | Interrupción controlada del productor. |
| `scripts/actualizar_costos.ps1` | Publicación de escenario económico. |
| `scripts/descargar_muestra_s01.py`, `descargar_historicos_s02.py` | Fuentes originales, checksums y manifiestos. |
| `scripts/preparar_s02.ps1`, `iniciar_s01.ps1`, `verificar_s01.ps1`, `verificar_s02.ps1` | Utilidades de las primeras sesiones. |
| `scripts/ejecutar_notebook_s01.py`, `ejecutar_notebook_s02.py` | Ejecución automatizada de prácticas. |
| `pyspark/sesiones/cryptopulse/core.py` | Rutas, variables, cortes, Spark y JSON atómico. |
| `.../quality.py`, `features.py`, `training.py` | Calidad, variables/etiquetas y aprendizaje/selección/calibración. |
| `.../contract.py`, `producer.py`, `stream.py` | Validación, captura/reconciliación y procesamiento vivo. |
| `.../inference.py`, `signals.py` | Aplicación de modelos, verdad futura y reglas explicadas. |
| `.../db.py`, `publish_batch.py` | Conexiones/idempotencia y publicación histórica. |
| `.../exporter.py`, `verify.py`, `integration.py` | API/métricas, controles e integración. |
| `.../test_*.py` | Ensayos de regresión y Spark descritos. |
| `pyspark/sesiones/s01-arquitectura`, `s02-fundamentos` | Notebooks iniciales, muestras, manifiestos y salidas prácticas. |
| `pyspark/sesiones/s03-calidad/artifacts` | Parquet, cuarentena, conflictos y huecos. |
| `pyspark/sesiones/s04-modelos/artifacts` | Datasets supervisados, modelos, registros, informes y test. |
| `pyspark/sesiones/_runtime` | Checkpoints, estado mutable, JAR y evidencias. |
| `docs/PRODUCTO_FINAL.md`, `GUIA_INICIO_LOCAL.md` | Guía operativa previa de la entrega corregida. |
| `docs/INFORME_PROYECTO_CRYPTOPULSE.md`, `INFORME_SESIONES_S01_S12.md`, `REVISION_PROYECTO_Y_GUIA_GRAFANA.md` | Historia de entregas/revisión; contrastar fecha y estado. |

En las filas abreviadas con `.../`, el prefijo es `pyspark/sesiones/cryptopulse/`. `.env` contiene configuración local y está excluido de Git. Una modificación hecha solo en la copia antigua de `outputs/CryptoPulse` del workspace no actualiza el proyecto de `D:\bigdata`.
