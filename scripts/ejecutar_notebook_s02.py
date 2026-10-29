"""Ejecutar dentro de cryptopulse-pyspark. No cierra otros kernels de Jupyter."""
import os
from pathlib import Path
import nbformat
from nbclient import NotebookClient

session = Path("/opt/s02-fundamentos")
artifacts = session / "artifacts"
artifacts.mkdir(exist_ok=True)
result = artifacts / "verificacion_s02.json"
# Quitar solo un resultado antiguo evita confundirlo con una corrida fallida.
result.unlink(missing_ok=True)
notebook = nbformat.read(session / "02_fundamentos_practica.ipynb", as_version=4)
# Se aplica antes de iniciar la JVM; no se depende de cambiar el heap con una JVM activa.
os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 2g pyspark-shell"
client = NotebookClient(notebook, timeout=360, kernel_name="python3",
                        resources={"metadata": {"path": str(session)}})
try:
    client.execute()
finally:
    nbformat.write(notebook, artifacts / "02_fundamentos_practica_ejecutado.ipynb")
if not result.exists():
    raise RuntimeError("La ultima celda no genero verificacion_s02.json")
print(result.read_text(encoding="utf-8"))
