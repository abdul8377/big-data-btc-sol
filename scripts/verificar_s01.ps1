$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$runnerPath = Join-Path $projectRoot 'scripts\ejecutar_notebook_s01.py'
Get-Content -LiteralPath $runnerPath -Raw | docker exec -i cryptopulse-pyspark python -
if ($LASTEXITCODE -ne 0) { throw 'La verificacion Spark no termino correctamente.' }
$resultPath = Join-Path $projectRoot 'pyspark\sesiones\s01-arquitectura\artifacts\verificacion_s01.json'
if (-not (Test-Path -LiteralPath $resultPath)) { throw 'No se encontro el resultado de la verificacion.' }
Get-Content -LiteralPath $resultPath
