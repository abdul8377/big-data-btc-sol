# Producto · Unidad I

## Base histórica y aprendizaje reproducible

S01–S05 preparan el entorno y el batch utilizado por la unidad 2. La muestra de S01 contiene 2.880 velas reales; el histórico completo contiene 114 archivos mensuales de enero de 2022 a septiembre de 2026.

De **4.993.759 entradas**, calidad conserva **4.993.757 válidas** y dos intervalos inválidos en cuarentena. Los tres huecos suman **163 minutos-activo entre ambos mercados**, no 163 por cada activo. No se interpolan precios.

Los CSV se verifican mediante SHA256 y se transforman en Parquet particionado por activo/año/mes. Se generan variables retrospectivas y etiquetas futuras, con particiones cronológicas. Se entrenan Random Forest, regresiones logísticas y ridge; se selecciona solo lo que mejora una referencia en validación.

| Sesión | Resultado | Código / artefacto |
|---|---|---|
| S01 | Docker, Jupyter y Spark; muestra real | `01_arquitectura_practica.ipynb` |
| S02 | Transformaciones, RDD, agregados y formatos | `02_fundamentos_practica.ipynb` |
| S03 | Calidad, cuarentena, huecos y Parquet | `quality.py`, `_runtime/s03.json` |
| S04 | Cuatro objetivos y métodos comparados | `training.py`, `model_registry.json` |
| S05 | Integración y recarga verificadas | `python -m cryptopulse.training --verify-u1` |

S05 se verifica con el módulo de entrenamiento y su opción `--verify-u1`; `verificar_producto.ps1` corresponde al cierre integral S12.

[Datos y calidad detallados](pipeline.md) · [Variables, modelos y test](modelos.md) · [Avance por sesión](sesiones.md).
