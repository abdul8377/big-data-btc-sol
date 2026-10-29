# Ejecución y reproducibilidad

## Requisitos

Windows con Docker Desktop y PowerShell. Los scripts usan rutas relativas desde la raíz del proyecto. Revisa el archivo de ejemplo para preparar parámetros locales; no publiques credenciales.

## Inicio y prueba

~~~powershell
Set-Location D:\bigdata
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\iniciar_producto.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verificar_producto.ps1
~~~

La documentación del repositorio indica los siguientes servicios locales: **Grafana** puerto 43000, **Kafka UI** 48085, **Prometheus** 49090, **API** 48000, **JupyterLab** 4488. Son accesos locales, no enlaces públicos de este sitio.

<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · RUN-01</span>
<h4>Inicio de contenedores</h4>
<p>Consola mostrando la ejecución del script iniciar_producto.ps1 y comprobación de servicios.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/run-01.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · RUN-02</span>
<h4>Verificación integral</h4>
<p>Salida del script verificar_producto.ps1 con comprobación del estado del pipeline y series.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/run-02.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · RUN-03</span>
<h4>Pruebas de integración</h4>
<p>Salida de pruebas_integracion.ps1 sobre duplicados, watermark, recuperación y temporalidad.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/run-03.png</code></span>
</div>
<div class="evidence-slot" markdown="1">
<span class="slot-head">IMAGEN PENDIENTE · RUN-04</span>
<h4>Vista local de Grafana</h4>
<p>Página CryptoPulse cargada desde localhost:43000 con series recientes.</p>
<span class="slot-file">Archivo sugerido: <code>assets/evidencias/run-04.png</code></span>
</div>

## Repetir el entrenamiento

~~~powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\entrenar_producto.ps1 -Descargar -Reprocesar -Reentrenar
~~~

## Apagar de forma controlada

~~~powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\detener_producto.ps1
~~~

## Recursos versionados

- [Guía de arranque](https://github.com/abdul8377/big-data-btc-sol/blob/main/docs/GUIA_INICIO_LOCAL.md)
- [Informe de producto](https://github.com/abdul8377/big-data-btc-sol/blob/main/docs/PRODUCTO_FINAL.md)
- [Scripts](https://github.com/abdul8377/big-data-btc-sol/tree/main/scripts)

No se publican datos sensibles de conexión. Cualquier vista de mercado debe acompañarse de fecha y zona horaria.
