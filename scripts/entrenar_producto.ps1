<#
.SYNOPSIS
Prepara y verifica el batch local de CryptoPulse.
.NOTES
Si falta Jupyter, inicia solo el entorno cryptopulse-pyspark. Si el stream de
este proyecto estaba activo, lo pausa durante el batch y lo restaura al salir.
Un contenedor ausente es un estado normal, no un error de Docker inspect.
Si Docker no responde o falla detener/restaurar el stream, se muestra el error
y no se anuncia "Batch verificado". Se conservan volumenes y checkpoints.
#>
param([switch]$Descargar,[switch]$Reprocesar,[switch]$Reentrenar)
$ErrorActionPreference='Stop'

function Test-CryptoPulseContainerRunning {
    param([Parameter(Mandatory=$true)]
          [ValidateSet('cryptopulse-pyspark','cryptopulse-streaming')][string]$Name)
    # docker inspect emite stderr para contenedores ausentes y PowerShell 5.1
    # con ErrorActionPreference=Stop aborta antes de comprobar LASTEXITCODE.
    $states=@(docker container ls -a --filter "name=^/$Name`$" --format '{{.State}}')
    if ($LASTEXITCODE -ne 0) { throw "No se pudo consultar Docker para $Name. Abre Docker Desktop y revisa el motor Linux." }
    return ($states -contains 'running')
}

$projectRoot=Split-Path -Parent $PSScriptRoot
$config=Get-Content -LiteralPath (Join-Path $projectRoot 'config\pipeline.json') -Raw | ConvertFrom-Json
$composeFile=Join-Path $projectRoot 'platform.yml'
if (-not (Test-CryptoPulseContainerRunning -Name 'cryptopulse-pyspark')) {
    & (Join-Path $PSScriptRoot 'iniciar_s01.ps1')
    if (-not (Test-CryptoPulseContainerRunning -Name 'cryptopulse-pyspark')) {
        throw 'El entorno PySpark no inicio. Revisa docker compose -f pyspark/compose.yml logs.'
    }
}
$manifest=Join-Path $projectRoot 'pyspark\sesiones\s02-fundamentos\data\provenance.json'
if ($Descargar -or -not (Test-Path -LiteralPath $manifest)) {
    & (Join-Path $PSScriptRoot 'preparar_s02.ps1') -Desde $config.history_from -Hasta $config.history_to
}
$resumeStreaming=Test-CryptoPulseContainerRunning -Name 'cryptopulse-streaming'
if ($resumeStreaming) {
    docker compose -f $composeFile stop streaming
    if ($LASTEXITCODE -ne 0) { throw 'No se pudo pausar el stream de CryptoPulse; no se inicia el batch.' }
}
try {
    $quality=Join-Path $projectRoot 'pyspark\sesiones\_runtime\s03.json'
    if ($Reprocesar -or $Descargar -or -not (Test-Path -LiteralPath $quality)) {
        docker exec -e PYTHONPATH=/opt cryptopulse-pyspark python -m cryptopulse.quality
        if ($LASTEXITCODE -ne 0) { throw 'Fallo S03.' }
        $Reentrenar=$true
    }
    $registry=Join-Path $projectRoot 'pyspark\sesiones\s04-modelos\artifacts\model_registry.json'
    if ($Reentrenar -or -not (Test-Path -LiteralPath $registry)) {
        docker exec -e PYTHONPATH=/opt cryptopulse-pyspark python -m cryptopulse.training
        if ($LASTEXITCODE -ne 0) { throw 'Fallo S04.' }
    }
    docker exec -e PYTHONPATH=/opt cryptopulse-pyspark python -m cryptopulse.training --verify-u1
    if ($LASTEXITCODE -ne 0) { throw 'Fallo la verificacion U1.' }
} finally {
    if ($resumeStreaming) {
        docker compose -f $composeFile up -d --no-deps --force-recreate streaming
        if ($LASTEXITCODE -ne 0) { throw 'No se pudo restaurar el stream de CryptoPulse. Ejecuta scripts/iniciar_producto.ps1 y revisa su estado.' }
    }
}
Write-Host 'Batch verificado. Tras cambiar modelos, publica KPIs, repite las pruebas de integracion y verifica el producto.'
