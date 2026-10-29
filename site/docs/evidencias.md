# Evidencias y galería técnica

## Aceptación reproducible guardada

La [evidencia estructurada descargable](assets/downloads/aceptacion-2026-10-28.json) resume la aceptación del **28 de octubre de 2026 a las 20:25:50 de Lima**. Incluye integración, paneles consultados y recuperación. No es un monitor de salud en directo.

| Comprobación | Resultado registrado |
|---|---|
| Regresiones de ingesta, contrato, señales, salud y calibración | 40 PASS |
| Spark con modelos guardados y velas reales | 5 PASS |
| Kafka, watermark, duplicados y checkpoint | PASS |
| Cuatro dimensiones, siete replays evaluados y temporalidad | PASS |
| Parada del productor durante 75 s | Degradación detectada y recuperación PASS |
| Consultas de Grafana | 16 SQL y siete Prometheus verificadas |

## Imágenes técnicas

![Arquitectura implementada](assets/images/arquitectura.svg)

*Diagrama documental generado a partir de servicios y conexiones reales.*

![Particiones cronológicas](assets/images/validacion-temporal.svg)

*Diagrama del método de validación; no es una captura del entrenamiento en ejecución.*

![Resultados históricos comparados](assets/images/errores-modelos.svg)

*Visualización de métricas guardadas en el registro. Cada objetivo conserva su escala.*

## Capturas de herramientas

Se documentará aquí la captura y fecha de cada interfaz que pueda verificarse en ejecución. El intento inicial de arranque del 29 de octubre encontró un bloqueo de acceso de Docker Desktop a sus sockets temporales. No se presentan capturas simuladas de Grafana, Prometheus, Kafka UI o Jupyter como si estuvieran funcionando.

[Ejecutar y verificar el producto](ejecucion.md) · [Detalle de pruebas y límites](limites.md).
