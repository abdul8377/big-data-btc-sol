$ErrorActionPreference='Stop'
$projectRoot=Split-Path -Parent $PSScriptRoot
docker compose --env-file (Join-Path $projectRoot '.env') -f (Join-Path $projectRoot 'platform.yml') run --rm --no-deps tools python -m cryptopulse.verify 12
if ($LASTEXITCODE -ne 0) { throw 'La comprobacion del producto no paso; revisa el error anterior.' }
