param(
    [string]$Desde = '2024-12',
    [string]$Hasta = '2025-01'
)
$ErrorActionPreference = 'Stop'
if ($Desde -notmatch '^\d{4}-\d{2}$' -or $Hasta -notmatch '^\d{4}-\d{2}$') {
    throw 'Usa meses AAAA-MM.'
}
$projectRoot = Split-Path -Parent $PSScriptRoot
$downloader = Join-Path $projectRoot 'scripts\descargar_historicos_s02.py'
$state = docker inspect cryptopulse-pyspark --format '{{.State.Running}}'
if ($LASTEXITCODE -ne 0 -or $state -ne 'true') { throw 'Inicia el entorno con scripts\iniciar_s01.ps1.' }
docker cp $downloader 'cryptopulse-pyspark:/tmp/cryptopulse_descargar_s02.py'
if ($LASTEXITCODE -ne 0) { throw 'No se pudo copiar el descargador.' }
docker exec -e CRYPTOPULSE_DATA_DIR=/opt/s02-fundamentos/data cryptopulse-pyspark python /tmp/cryptopulse_descargar_s02.py --desde $Desde --hasta $Hasta
if ($LASTEXITCODE -ne 0) { throw 'No se completo la descarga de historicos S02.' }
