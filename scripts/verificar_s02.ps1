$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$runnerPath = Join-Path $projectRoot 'scripts\ejecutar_notebook_s02.py'
Get-Content -LiteralPath $runnerPath -Raw | docker exec -i cryptopulse-pyspark python -
if ($LASTEXITCODE -ne 0) { throw 'La ejecucion de S02 fallo; revisa el notebook ejecutado.' }
$resultPath = Join-Path $projectRoot 'pyspark\sesiones\s02-fundamentos\artifacts\verificacion_s02.json'
$result = Get-Content -LiteralPath $resultPath -Raw | ConvertFrom-Json
if ($result.status -ne 'PASS') { throw 'La verificacion S02 no paso.' }
