"""Regresiones de señales, contexto y evaluación; no publican eventos ni escriben PG."""
import unittest
from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch
from .signals import build_signals, context_diagnostics, current_pair_anchor


def registry():
    return {"model_version":"unit-test", "thresholds":{
        "by_symbol":{s:{"volatility_alert_pct":0.5,"crash_threshold_pct":-1.0} for s in ("BTCUSDT","SOLUSDT")},
        "divergence_alert_pp":0.8}, "models":{
        "volatility":{"selected":"random_forest"},
        "crash":{"selected":"logistic_regression","alert_threshold":0.06},
        "anomaly":{"selected":"logistic_regression","alert_threshold":0.34},
        "divergence":{"selected":"baseline"}}}


class SignalsTests(unittest.TestCase):
    def kpi(self, **changes):
        row={"rv15":0.1,"r30":0.2,"quote5":50.0,"quote5_p95":100.0,
             "trades5":50,"trades5_p95":100,"source_mode":"live"}
        row.update(changes)
        return ("BTCUSDT",59999,row)

    def test_calm_market_never_generates_alert_just_to_fill_panel(self):
        result=build_signals(registry(),[self.kpi()],[])
        self.assertEqual(len(result),3)
        self.assertTrue(all(not s["triggered"] and s["severity"]=="normal" for s in result))
        self.assertTrue(all(s["explanation"] and s["action"] and s["expires_ms"]>s["as_of_ms"] for s in result))

    def test_observed_crash_and_predicted_crash_are_distinct(self):
        prediction=("crash","BTCUSDT",59999,30,0.01,0.05,-1.2,"unit-test","logistic_regression","live",False)
        result=build_signals(registry(),[self.kpi(r30=-1.2)],[prediction])
        crash={s["kind"]:s for s in result if s["dimension"]=="crash"}
        self.assertTrue(crash["observed"]["triggered"])
        self.assertFalse(crash["forecast"]["triggered"])
        self.assertEqual(crash["observed"]["unit"],"pct")
        self.assertEqual(crash["forecast"]["unit"],"probability")
        self.assertEqual(crash["forecast"]["threshold"],0.06)
        self.assertIn("no una caída",crash["forecast"]["explanation"])

    def test_boundary_uses_saved_historical_threshold(self):
        result=build_signals(registry(),[self.kpi(rv15=0.5,r30=-1.0,quote5=100)],[])
        self.assertTrue(all(s["triggered"] for s in result))
        self.assertEqual(next(s for s in result if s["dimension"]=="anomaly")["value"],1.0)

    def test_either_activity_measure_can_cross_reference(self):
        result=build_signals(registry(),[self.kpi(quote5=30,trades5=200)],[])
        activity=next(s for s in result if s["dimension"]=="anomaly")
        self.assertTrue(activity["triggered"])
        self.assertEqual(activity["value"],2.0)

    def test_missing_activity_is_unavailable_not_a_false_normal(self):
        result=build_signals(registry(),[self.kpi(quote5=None)],[])
        activity=next(s for s in result if s["dimension"]=="anomaly")
        self.assertEqual(activity["severity"],"unavailable")
        self.assertFalse(activity["triggered"])
        self.assertIsNone(activity["value"])

    def test_divergence_observation_useful_even_when_forecast_is_zero_baseline(self):
        prediction=("divergence","BTC-SOL",59999,15,0,0,-1.2,"unit-test","baseline","live",False)
        result=build_signals(registry(),[("BTC-SOL",59999,{"current_spread15":-1.2,"source_mode":"live"})],[prediction])
        self.assertTrue(result[0]["triggered"])
        self.assertEqual(result[0]["value"],-1.2)
        self.assertEqual(result[0]["threshold"],0.8)
        self.assertIn("|SOL menos BTC|",result[0]["explanation"])
        self.assertEqual(result[1]["severity"],"unavailable")
        self.assertFalse(result[1]["triggered"])

    def test_replay_and_backfill_provenance_is_preserved(self):
        for mode in ("replay","backfill"):
            result=build_signals(registry(),[self.kpi(source_mode=mode,rv15=1.0)],[])
            self.assertTrue(all(s["source_mode"]==mode for s in result))

    def test_one_missing_minute_blocks_full_features_despite_recent_price(self):
        rows=[{"symbol":"BTCUSDT","open_ms":i*60000,"close_ms":i*60000+59999} for i in range(61)]
        complete=context_diagnostics(rows)["BTCUSDT"]
        missing=context_diagnostics([r for r in rows if r["open_ms"]!=30*60000])["BTCUSDT"]
        self.assertTrue(complete["feature_ready"])
        self.assertFalse(missing["feature_ready"])
        self.assertEqual(missing["missing_minutes"],1)
        self.assertEqual(complete["latest_close_ms"],missing["latest_close_ms"])

    def test_pair_never_falls_back_to_old_shared_time(self):
        self.assertIsNone(current_pair_anchor({"BTCUSDT":{"open_ms":120000},"SOLUSDT":{"open_ms":60000}}))
        self.assertEqual(current_pair_anchor({"BTCUSDT":{"open_ms":120000},"SOLUSDT":{"open_ms":120000}}),120000)


class EvaluationTests(unittest.TestCase):
    def setUp(self):
        from . import inference
        self.module=inference
        self.executed=[]

    @contextmanager
    def connection(self):
        parent=self
        class Cursor:
            def __enter__(self): return self
            def __exit__(self,*_): return False
            def execute(self,sql,params): parent.executed.append((sql,params))
        yield SimpleNamespace(cursor=lambda:Cursor())

    def prediction(self, version="old", asof=59999):
        return {"dimension":"crash","symbol":"BTCUSDT","as_of_ms":asof,
                "horizon_minutes":1,"model_version":version}

    def candles(self):
        return [{"symbol":"BTCUSDT","open_ms":0,"close":"100"},
                {"symbol":"BTCUSDT","open_ms":60000,"close":"98.5"}]

    def test_old_prediction_evaluated_using_its_own_version_threshold(self):
        current=registry(); current["model_version"]="current"
        old=deepcopy(current); old["model_version"]="old"
        old["thresholds"]["by_symbol"]["BTCUSDT"]["crash_threshold_pct"]=-2.0
        def query(sql,params=None):
            if "SELECT p.*" in sql: return [self.prediction()]
            if "model_versions" in sql: return [{"registry":old}]
            return self.candles()
        with patch.object(self.module.db,"query",side_effect=query),patch.object(self.module.db,"connect",self.connection):
            self.assertEqual(self.module.evaluate_matured(SimpleNamespace(registry=current)),1)
        # -1.5% is positive under current -1% but negative under original -2%.
        self.assertEqual(self.executed[0][1][0],0.0)

    def test_unknown_version_is_explicit_not_scored_with_current_thresholds(self):
        current=registry()
        with patch.object(self.module.db,"query",side_effect=[[self.prediction()],[]]),patch.object(self.module.db,"connect",self.connection):
            self.assertEqual(self.module.evaluate_matured(SimpleNamespace(registry=current)),0)
        self.assertIn("unknown_model",self.executed[0][0])

    def test_missing_future_does_not_block_other_ready_prediction(self):
        current=registry()
        first=self.prediction("unit-test",119999)
        second=self.prediction("unit-test",59999)
        with patch.object(self.module.db,"query",side_effect=[[first,second],[],self.candles()]),patch.object(self.module.db,"connect",self.connection):
            self.assertEqual(self.module.evaluate_matured(SimpleNamespace(registry=current)),1)
        self.assertIn("missing_data",self.executed[0][0])
        self.assertIn("evaluation_status='evaluated'",self.executed[1][0])


if __name__=="__main__":
    unittest.main()
