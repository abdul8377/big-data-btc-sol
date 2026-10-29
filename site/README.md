# Sitio académico CryptoPulse

Documentación MkDocs Material separada del código y los informes existentes.

## Vista previa en Windows

Desde la raíz del repositorio:

~~~powershell
py -m pip install -r site/requirements.txt
py -m mkdocs serve -f site/mkdocs.yml
~~~

Visita http://127.0.0.1:8000/big-data-btc-sol/ o la dirección que indique la terminal.

## Publicación

1. Fusiona la rama de documentación con main.
2. Abre Settings > Pages en GitHub.
3. Selecciona Source: GitHub Actions.
4. Ejecuta el workflow «CryptoPulse · Documentación MkDocs» si no inició automáticamente.
5. El sitio esperado será https://abdul8377.github.io/big-data-btc-sol/.

No se publica Grafana localhost ni información de acceso. El sitio es estático.

## Sustituir espacios para imágenes

Cada recuadro tiene un código único como U2-08. Busca el código en site/docs/.
Coloca tu captura en site/docs/assets/evidencias/ (crea la carpeta si aún no existe),
y reemplaza el bloque <div class="evidence-slot"> correspondiente por:

~~~markdown
![U2-08 · Captura de procesamiento streaming](assets/evidencias/u2-08-streaming.png)
*Figura U2-08. Descripción de lo que muestra la captura.*
~~~

Conserva el nombre, la descripción, el contexto y el vínculo al código que sustenta
la captura. No reutilices una sola captura como prueba de varias capacidades si no demuestra cada una.

## Alcance y veracidad

Los textos están elaborados a partir de README.md, docs/PRODUCTO_FINAL.md,
docs/INFORME_SESIONES_S01_S12.md y módulos del repositorio.
Las verificaciones y números publicados describen el estado documentado del repositorio,
no el estado en vivo de tu ordenador. Las rúbricas muestran criterios sin notas inventadas.

La S07 utiliza datos continuos del mercado como análogo de telemetría y NO
demuestra por sí sola que haya un dispositivo IoT físico. Revisa con el docente
si esta adaptación es aceptada para su criterio específico.
