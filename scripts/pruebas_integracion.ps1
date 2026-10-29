$ErrorActionPreference='Stop'
$projectRoot=Split-Path -Parent $PSScriptRoot
$submitArgs='PYSPARK_SUBMIT_ARGS=--master local[2] --driver-memory 1g --conf spark.jars.ivy=/opt/_runtime/ivy --conf spark.jars.ivySettings=/opt/cryptopulse/ivysettings.xml --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 pyspark-shell'
docker compose --env-file (Join-Path $projectRoot '.env') -f (Join-Path $projectRoot 'platform.yml') run --rm --no-deps -e $submitArgs tools python -m cryptopulse.integration
if ($LASTEXITCODE -ne 0) { throw 'Las pruebas aisladas de integracion fallaron.' }
