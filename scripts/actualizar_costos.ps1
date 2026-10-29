$ErrorActionPreference='Stop'
$projectRoot=Split-Path -Parent $PSScriptRoot
$assumptions=Get-Content -LiteralPath (Join-Path $projectRoot 'config\cost-assumptions.json') -Raw | ConvertFrom-Json
$monthly=$assumptions.hours_per_month*($assumptions.cpu_cores*$assumptions.cpu_core_hour_usd+$assumptions.ram_gib*$assumptions.ram_gib_hour_usd)+$assumptions.disk_gib*$assumptions.disk_gib_month_usd
$result=@{monthly_usd=$monthly;assumptions=$assumptions;updated_at_utc=[DateTime]::UtcNow.ToString('o');kind='illustrative_estimate'}
$destination=Join-Path $projectRoot 'pyspark\sesiones\_runtime\costs.json'
[IO.File]::WriteAllText($destination,($result | ConvertTo-Json -Depth 5),[Text.UTF8Encoding]::new($false))
Write-Host ('Estimacion ilustrativa: USD {0:N2}/mes' -f $monthly)
