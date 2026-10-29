# CryptoPulse: iniciar el proyecto local

El proyecto está en `D:\bigdata`. Kafka, Spark Streaming, PostgreSQL, Grafana, Prometheus y los demás servicios se ejecutan en contenedores Linux de Docker Desktop. Jupyter tiene su propio archivo Compose.

## 1. Abrir Docker Desktop

Abre Docker Desktop desde el menú Inicio y espera a que indique que el motor está funcionando. Utiliza contenedores Linux. Abre PowerShell y comprueba:

```powershell
docker version
```

Debe mostrar las secciones `Client` y `Server`. Si solo aparece `Client` o un error de conexión, el motor todavía no está disponible.

## 2. Iniciar CryptoPulse

En PowerShell ejecuta:

```powershell
Set-Location D:\bigdata
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\iniciar_producto.ps1
```

Este comando inicia la plataforma con su configuración existente. No necesitas volver a descargar los históricos ni entrenar los modelos para un arranque normal. Necesitas conexión a Internet para recibir datos públicos de Binance; no se requiere una clave API.

El inicio aplica las migraciones conservando los volúmenes, publica la versión activa de modelos y espera hasta cuatro minutos a que los datos sean consecutivos y las siete series de pronósticos estén recientes. Si el producto no alcanza esas condiciones, informa el fallo; tener contenedores activos no basta para anunciar que está listo.

Espera a que Kafka y PostgreSQL estén saludables y Spark termine de cargar los modelos. Tras un período apagado, el productor recupera velas mediante REST; los paneles pueden tardar unos minutos en mostrar datos recientes.

## 3. Abrir las interfaces

| Interfaz | Dirección | Acceso |
|---|---|---|
| Grafana, tablero del producto | http://localhost:43000/d/cryptopulse-main | Usuario `admin`, contraseña `admin2026!` |
| Kafka UI | http://localhost:48085 | Sin contraseña configurada |
| Prometheus | http://localhost:49090 | Sin contraseña configurada |
| Estado del pipeline | http://localhost:48000/api/status | JSON |

Grafana muestra precios, indicadores, pronósticos y el estado operativo. Las nuevas predicciones se evalúan cuando transcurren sus horizontes de 5, 15 o 30 minutos y hay datos suficientes. Mantén el rango del tablero cerca de la hora actual y activa su actualización automática.

El centro de señales contiene 14 evaluaciones (observación y pronóstico de cada dimensión/activo). `NORMAL` indica una evaluación reciente sin cruce; `REFERENCIA` identifica el pronóstico de divergencia cero; `DESACTUALIZADO` o `SIN EVALUAR` requiere revisar datos. El historial explica cuando no hubo cruces de umbral: no necesita mostrar alertas todo el tiempo para funcionar.

Consulta [docs/PRODUCTO_FINAL.md](PRODUCTO_FINAL.md) para interpretar el tablero renovado. El estado de señales también está en http://localhost:48000/api/signals.

## 4. Comprobar el funcionamiento

```powershell
Set-Location D:\bigdata
docker compose -f platform.yml ps
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verificar_producto.ps1
```

La comprobación completa debe terminar con `"session": "S12"` y `"status": "PASS"`. Ejecútala cuando haya terminado la recuperación inicial. El script consulta los servicios, las fuentes de Grafana y la frescura de los datos; también comprueba los resultados guardados de las pruebas de integración anteriores.

Para consultar mensajes recientes del procesamiento:

```powershell
docker compose -f platform.yml logs --tail 50 producer streaming
```

## 5. Abrir Jupyter si quieres trabajar en los notebooks

Jupyter se administra por separado y no es necesario para consultar Grafana:

```powershell
Set-Location D:\bigdata
docker compose -f pyspark\compose.yml up -d
```

Abre http://localhost:4488/lab?token=sintoken. Si solicita un token, introduce `sintoken`.

## 6. Detener el proyecto conservando sus datos

```powershell
Set-Location D:\bigdata
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\detener_producto.ps1
docker compose -f pyspark\compose.yml stop
```

El segundo comando detiene también Jupyter. Después puedes cerrar Docker Desktop. Estos comandos conservan los volúmenes, históricos, modelos y checkpoints. Para volver a trabajar, repite los pasos de inicio.

## Si algo falla

- **Error de conexión con Docker:** abre Docker Desktop y comprueba que `docker version` muestre `Server` antes de iniciar el proyecto.
- **Grafana abre pero los paneles no tienen datos recientes:** espera unos minutos, revisa el rango de tiempo y consulta los logs de `producer` y `streaming`.
- **La verificación falla justo después de arrancar:** espera a que Spark cargue los modelos y se complete la recuperación de velas; vuelve a verificar. Si persiste, conserva el mensaje de error y los logs para diagnosticarlo.
- **Puerto ocupado:** consulta `docker compose -f platform.yml ps` y revisa qué aplicación utiliza ese puerto antes de modificar la configuración.

El 28 de octubre de 2026 se detectaron sockets temporales inaccesibles que impedían arrancar Docker Desktop. Se apartaron con respaldo sus directorios temporales para regenerarlos. Fue una recuperación del motor; no forma parte del inicio habitual del proyecto. Si Docker Desktop vuelve a mostrar ese error, conserva su mensaje para revisar el caso antes de aplicar cambios.

Código, históricos, modelos y checkpoints están en `D:\bigdata`. Las bases de datos y otros servicios persistentes usan volúmenes administrados por Docker Desktop. Los informes extensos están en `docs\INFORME_PROYECTO_CRYPTOPULSE.md` y `docs\INFORME_SESIONES_S01_S12.md`.
