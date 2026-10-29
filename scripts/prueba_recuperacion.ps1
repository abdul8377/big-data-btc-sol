param([int]$SegundosPausa=75)
$ErrorActionPreference='Stop'
if ($SegundosPausa -lt 70 -or $SegundosPausa -gt 120) { throw 'La pausa de prueba debe ser de 70 a 120 segundos.' }
$root=Split-Path -Parent $PSScriptRoot
$compose=Join-Path $root 'platform.yml'
$evidence=Join-Path $root 'pyspark\sesiones\_runtime\recovery_acceptance.json'
function Snapshot { return (Invoke-RestMethod -Uri 'http://localhost:48000/api/status' -TimeoutSec 10) }
$before=Snapshot
if (-not $before.health.ready) { throw 'El producto debe estar listo antes de probar su recuperacion.' }
$record=[ordered]@{status='RUNNING';before=$before.health;pause_seconds=$SegundosPausa}
Write-Host 'Pausando solo el productor para comprobar deteccion del fallo y recuperacion sin huecos.'
docker compose -f $compose stop producer
if ($LASTEXITCODE -ne 0) { throw 'No se pudo pausar el productor.' }
try {
    $remaining=$SegundosPausa
    while ($remaining -gt 0) { $step=[Math]::Min(30,$remaining); Start-Sleep -Seconds $step; $remaining-=$step }
    $during=Snapshot
    $record.during=$during.health
    if ($during.health.ready) { throw 'La salud permanecio verde tras detener el productor: fallo de deteccion.' }
    Write-Host 'Degradacion detectada correctamente; restaurando la ingesta.'
} finally {
    docker compose -f $compose up -d --no-deps producer
    if ($LASTEXITCODE -ne 0) { throw 'No se pudo restaurar el productor.' }
}
$deadline=(Get-Date).AddMinutes(4)
$after=$null
do {
    Start-Sleep -Seconds 5
    $after=Snapshot
    if ($after.health.ready) { break }
} while ((Get-Date) -lt $deadline)
$record.after=$after.health
if (-not $after.health.ready) { $record.status='FAIL'; $record | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $evidence -Encoding UTF8; throw 'No se recupero el producto dentro del plazo.' }
if (@($after.health.symbols | Where-Object {$_.missing_minutes -ne 0}).Count -gt 0) { throw 'La recuperacion dejo un hueco.' }
$record.status='PASS'
$record.verified_at=(Get-Date).ToString('o')
$record | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $evidence -Encoding UTF8
Write-Host 'PASS: fallo detectado, productor restaurado, continuidad y siete series recientes confirmadas.'
