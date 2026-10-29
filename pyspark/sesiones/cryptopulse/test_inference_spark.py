"""Pruebas con modelos guardados y velas reales; sinks simulados, no escritura PG."""
import csv
import math
import os
import unittest
from unittest.mock import patch


@unittest.skipUnless(os.environ.get("CRYPTOPULSE_SPARK_TESTS")=="1", "Activar explícitamente la prueba Spark")
class SavedModelInferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from .core import ROOT, get_spark
        from .inference import ModelBundle
        cls.spark=get_spark("inference-regression",memory="1g",cores=2)
        cls.rows=[]
        for symbol in ("BTCUSDT","SOLUSDT"):
            with (ROOT/f"s02-fundamentos/data/raw/{symbol}-1m-2026-01.csv").open() as source:
                reader=csv.reader(source)
                for _ in range(120):
                    c=next(reader)
                    cls.rows.append({"symbol":symbol,"open_ms":int(c[0])//1000,"close_ms":int(c[6])//1000,
                        "open":c[1],"high":c[2],"low":c[3],"close":c[4],"volume":c[5],
                        "quote_asset_volume":c[7],"number_of_trades":int(c[8]),"source_mode":"replay"})
        with patch("cryptopulse.inference.db.ensure_product_schema"):
            cls.bundle=ModelBundle()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def predict(self, rows):
        with patch("cryptopulse.inference.db.insert_kpis"),patch("cryptopulse.inference.db.insert_signals") as signals,patch("cryptopulse.inference.save_json"):
            predictions=self.bundle.predict(self.spark,rows)
            return predictions,signals.call_args.args[0]

    def test_saved_models_emit_seven_series_and_fourteen_explainable_assessments(self):
        predictions,signals=self.predict(self.rows)
        self.assertEqual(len(predictions),7)
        self.assertEqual(len(signals),14)
        self.assertEqual(self.bundle.last_status["status"],"ready")
        self.assertEqual({p[0] for p in predictions},{"volatility","crash","anomaly","divergence"})
        self.assertTrue(all(s["source_mode"]=="replay" for s in signals))
        self.assertTrue(all(s["value"] is not None for s in signals))

    def test_lagging_asset_never_emits_old_pair_as_current(self):
        latest=max(r["open_ms"] for r in self.rows)
        rows=[r for r in self.rows if not (r["symbol"]=="SOLUSDT" and r["open_ms"]==latest)]
        predictions,signals=self.predict(rows)
        self.assertEqual(len(predictions),6)
        self.assertFalse(any(p[0]=="divergence" for p in predictions))
        self.assertFalse(self.bundle.last_status["pair_synchronized"])

    def test_recovering_missing_real_minute_restores_predictions(self):
        latest=max(r["open_ms"] for r in self.rows)
        missing=latest-30*60000
        incomplete=[r for r in self.rows if r["open_ms"]!=missing]
        predictions,_=self.predict(incomplete)
        self.assertEqual(predictions,[])
        self.assertEqual(self.bundle.last_status["status"],"degraded")
        self.assertTrue(all(s["missing_minutes"]==1 for s in self.bundle.last_status["per_asset"].values()))
        restored,_=self.predict(self.rows)
        self.assertEqual(len(restored),7)

    def test_activity_windows_reject_five_rows_spanning_six_minutes(self):
        from .features import past_features
        from .inference import CANDLE_SCHEMA
        from pyspark.sql import functions as F
        btc=[r for r in self.rows if r["symbol"]=="BTCUSDT"][:6]
        # Five real observations, with the third minute deliberately omitted.
        rows=[r for i,r in enumerate(btc) if i!=2]
        values=[(r["symbol"],r["open_ms"],r["close_ms"],*[float(r[k]) for k in
            ("open","high","low","close","volume","quote_asset_volume")],r["number_of_trades"],r["source_mode"]) for r in rows]
        computed=past_features(self.spark.createDataFrame(values,CANDLE_SCHEMA))
        latest=computed.filter(F.col("open_ms")==btc[-1]["open_ms"]).select("quote5","trades5").first()
        self.assertIsNone(latest.quote5)
        self.assertIsNone(latest.trades5)

    def test_sixty_one_complete_real_minutes_match_manual_model_features(self):
        from .core import FEATURES
        from .features import past_features
        from .inference import CANDLE_SCHEMA
        from datetime import datetime,timezone
        from pyspark.sql import functions as F
        rows=[r for r in self.rows if r["symbol"]=="BTCUSDT"][:61]
        values=[(r["symbol"],r["open_ms"],r["close_ms"],*[float(r[k]) for k in
            ("open","high","low","close","volume","quote_asset_volume")],r["number_of_trades"],r["source_mode"]) for r in rows]
        actual=past_features(self.spark.createDataFrame(values,CANDLE_SCHEMA)).filter(
            F.col("open_ms")==rows[-1]["open_ms"]).select(*FEATURES,"quote5","quote60","trades5","trades60").first()
        prices=[float(r["close"]) for r in rows]
        expected={"r"+str(n):100*(prices[-1]/prices[-1-n]-1) for n in (1,5,15,30)}
        for n in (15,60):
            expected["rv"+str(n)]=100*math.sqrt(sum(math.log(prices[i]/prices[i-1])**2 for i in range(len(prices)-n,len(prices))))
        for n in (5,60):
            expected["quote"+str(n)]=sum(float(r["quote_asset_volume"]) for r in rows[-n:])
            expected["trades"+str(n)]=sum(r["number_of_trades"] for r in rows[-n:])
        end=rows[-1]
        hour=datetime.fromtimestamp(end["close_ms"]/1000,timezone.utc).hour
        expected.update({"log_quote5":math.log1p(expected["quote5"]),"log_trades5":math.log1p(expected["trades5"]),
            "volume_ratio":12*expected["quote5"]/expected["quote60"],
            "range_pct":100*(float(end["high"])-float(end["low"]))/float(end["open"]),
            "body_pct":100*(prices[-1]-float(end["open"]))/float(end["open"]),
            "hour_sin":math.sin(hour*2*math.pi/24),"hour_cos":math.cos(hour*2*math.pi/24),"is_sol":0.0})
        for name in expected:
            self.assertTrue(math.isclose(actual[name],expected[name],rel_tol=1e-10,abs_tol=1e-10),name)


if __name__=="__main__":
    unittest.main()
