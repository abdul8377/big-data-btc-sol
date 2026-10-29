"""S11: publicar KPIs batch y metricas reales de modelos para Grafana."""
import json
from pyspark.sql import functions as F
from psycopg2.extras import execute_values,Json
from .core import ML,CLEAN,get_spark
from . import db

def publish_metadata():
    registry=json.loads((ML/"model_registry.json").read_text())
    metrics=[]
    for dimension,item in registry["models"].items():
        for split in ("test_candidate","test_baseline","test_deployed"):
            if split in item:
                method="baseline" if split=="test_baseline" else item["selected"] if split=="test_deployed" else item.get("candidate_name","logistic_regression")
                metrics.append((dimension,split,method,registry["model_version"],Json(item[split])))
    with db.connect() as connection,connection.cursor() as cursor:
        execute_values(cursor,"""INSERT INTO model_metrics(dimension,split,method,model_version,metrics) VALUES %s
            ON CONFLICT(dimension,split,method,model_version) DO UPDATE SET metrics=EXCLUDED.metrics""",metrics)
    db.ensure_product_schema(registry)
    return registry,len(metrics)

def run():
    registry,metric_count=publish_metadata()
    spark=get_spark("s11-batch-kpis",memory="1g",cores=2)
    rows=spark.read.parquet(str(CLEAN)).groupBy("symbol","date_utc").agg(F.count("*").alias("candles"),
        F.sum("quote_asset_volume").alias("quote_volume_usdt"),F.sum("number_of_trades").alias("trades")).collect()
    with db.connect() as connection,connection.cursor() as cursor:
        execute_values(cursor,"""INSERT INTO batch_kpis(symbol,date_utc,candles,quote_volume_usdt,trades) VALUES %s
            ON CONFLICT(symbol,date_utc) DO UPDATE SET candles=EXCLUDED.candles,
            quote_volume_usdt=EXCLUDED.quote_volume_usdt,trades=EXCLUDED.trades""",
            [(r.symbol,r.date_utc,r.candles,r.quote_volume_usdt,r.trades) for r in rows])
    spark.stop()
    result={"batch_daily_rows":len(rows),"model_metric_rows":metric_count,"model_version":registry["model_version"]}
    print(json.dumps(result),flush=True)
    return result

if __name__=="__main__":
    import sys
    if '--metadata-only' in sys.argv:
        registry,count=publish_metadata()
        print(json.dumps({'model_version':registry['model_version'],'model_metric_rows':count}))
    else:
        run()
