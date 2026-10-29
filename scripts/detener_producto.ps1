$ErrorActionPreference='Stop'
$projectRoot=Split-Path -Parent $PSScriptRoot
docker compose --env-file (Join-Path $projectRoot '.env') -f (Join-Path $projectRoot 'platform.yml') stop
if ($LASTEXITCODE -ne 0) { throw 'No se pudieron detener todos los servicios CryptoPulse.' }
Write-Host 'Servicios CryptoPulse detenidos. Se conservaron datos, modelos y checkpoints.'
