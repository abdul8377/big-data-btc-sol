param([switch]$Reconstruir)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $projectRoot 'pyspark\compose.yml'
$envFile = Join-Path $projectRoot 'pyspark\.env'
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'Instala Docker Desktop antes de iniciar el laboratorio.'
}
docker info --format '{{.ServerVersion}}'
if ($LASTEXITCODE -ne 0) {
    throw 'Abre Docker Desktop, espera a que el motor Linux este listo y ejecuta este script en PowerShell de tu usuario de Windows.'
}
if (-not (Test-Path -LiteralPath $envFile)) {
    Copy-Item -LiteralPath (Join-Path $projectRoot 'pyspark\.env.example') -Destination $envFile
}
$csvPath = Join-Path $projectRoot 'pyspark\sesiones\s01-arquitectura\data\cryptopulse_klines_muestra.csv'
if (-not (Test-Path -LiteralPath $csvPath)) {
    throw 'Falta la muestra. Consulta README.md para descargarla.'
}
if ($Reconstruir) {
    docker compose --env-file $envFile -f $composeFile up -d --build
} else {
    docker compose --env-file $envFile -f $composeFile up -d
}
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose no pudo iniciar. Revisa el error anterior.' }
docker compose --env-file $envFile -f $composeFile ps
Write-Host 'JupyterLab: http://localhost:4488/lab?token=sintoken'
Write-Host 'Abre s01-arquitectura/01_arquitectura_practica.ipynb y ejecuta todas las celdas.'
Write-Host 'Spark UI: http://localhost:4042 (despues de crear SparkSession).'
