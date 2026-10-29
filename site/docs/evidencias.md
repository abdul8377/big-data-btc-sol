# Catálogo de capturas pendientes

Este sitio se entrega **sin imágenes de ejecución** hasta que se incorporen capturas reales. Los recuadros visibles en cada sección describen exactamente qué capturar. Se pueden añadir tantas evidencias adicionales como sean necesarias, asignando códigos consecutivos.

| Página | Tipo de evidencia | Códigos |
|---|---|---|
| Inicio | Vista del producto y arquitectura | HOME-01, HOME-02 |
| Brief | Problema, fuente y plan | BRIEF-01 a BRIEF-03 |
| Arquitectura | Componentes, flujos y operación | ARQ-01 a ARQ-06 |
| Modelos | Variables, temporalidad y evaluación | MOD-01 a MOD-06 |
| Unidad I | Ejecución batch por sesión | U1-01 a U1-08 |
| Unidad II | Pipeline streaming e integración | U2-01 a U2-20 |
| Producto final | Demostración integral | U3-01 a U3-05 |
| Grafana | Paneles detallados | GRAF-01 a GRAF-11 |
| Sesiones | Índice técnico | SES-01, SES-02 |
| Ejecución | Arranque, verificación y pruebas | RUN-01 a RUN-04 |

<div class="cp-callout" markdown="1">**Regla de evidencias:** no reemplazar un resultado faltante por una captura artificial; si aún no se ejecutó una prueba, conservar el marcador hasta obtener la evidencia. Se admite más de una imagen por punto si permite leer mejor los detalles.</div>

## Ejemplo de reemplazo

El marcador U2-08 pide una captura de watermark. Guárdala como `site/docs/assets/evidencias/u2-08.png` y sustituye el bloque que contiene «U2-08» por:

~~~markdown
![U2-08 · Watermark y ventanas](assets/evidencias/u2-08.png)

*Figura U2-08. Ventanas temporales procesadas por Structured Streaming. Fuente: ejecución propia.*
~~~

La documentación muestra automáticamente las imágenes al publicar de nuevo. Evita credenciales y tokens en las capturas.
