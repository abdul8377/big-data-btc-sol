"""Verificacion opcional DENTRO del contenedor. Cierra el kernel al terminar.
Para capturar Spark UI usa JupyterLab y deja su kernel activo.
"""
import json
from pathlib import Path
from nbclient import NotebookClient
import nbformat

session = Path('/opt/s01-arquitectura')
destination = session / 'artifacts/01_arquitectura_practica_ejecutado.ipynb'
destination.parent.mkdir(parents=True, exist_ok=True)
notebook = nbformat.read(session / '01_arquitectura_practica.ipynb', as_version=4)
NotebookClient(notebook, timeout=180, kernel_name='python3',
               resources={'metadata': {'path': str(session)}}).execute()
nbformat.write(notebook, destination)
errors = [out for cell in notebook.cells if cell.cell_type == 'code'
          for out in cell.outputs if out.output_type == 'error']
if errors:
    raise RuntimeError(json.dumps(errors, ensure_ascii=False))
print(f'Notebook ejecutado sin errores: {destination}')
