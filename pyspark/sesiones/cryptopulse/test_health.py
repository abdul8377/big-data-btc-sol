"""Regresiones: precios frescos nunca ocultan inferencia detenida o huecos."""
import copy
import io
import json
import unittest
from unittest.mock import patch
from .exporter import build_health,EXPECTED_SERIES,Handler
from .verify import validate_panel_result

NOW=1800000000000
LAST=NOW-15001
OPEN=LAST-59999
VERSION="regression-fixture"

def fixture():
    candles=[{"symbol":symbol,"last_close_ms":LAST,"candles":1000}
             for symbol in ("BTCUSDT","SOLUSDT")]
    context=[{"symbol":symbol,"open_ms":OPEN-i*60000}
             for symbol in ("BTCUSDT","SOLUSDT") for i in range(61)]
    predictions=[{"dimension":dimension,"symbol":symbol,"as_of_ms":LAST,"model_version":VERSION}
                 for dimension,symbol in EXPECTED_SERIES]
    stream={"heartbeat_ms":NOW-2000,"model_version":VERSION,"inference_enabled":True}
    producer={"heartbeat_ms":NOW-3000,"connected":True}
    return candles,context,predictions,stream,producer

class HealthRegressionTests(unittest.TestCase):
    def health(self,values):
        return build_health(*values,VERSION,current_ms=NOW)

    def test_complete_context_and_seven_fresh_series_are_ready(self):
        result=self.health(fixture())
        self.assertTrue(result["ready"])
        self.assertEqual(7,len(result["prediction_series"]))
        self.assertEqual([],result["reasons"])

    def test_fresh_market_does_not_hide_thirty_minute_old_predictions(self):
        values=list(fixture())
        for row in values[2]:
            row["as_of_ms"]-=30*60000
        result=self.health(values)
        self.assertFalse(result["ready"])
        self.assertTrue(all(asset["context_ready"] for asset in result["symbols"]))
        self.assertTrue(all("stale" in row["reasons"] and "behind_market" in row["reasons"]
                            for row in result["prediction_series"]))

    def test_one_missing_minute_blocks_only_relevant_context(self):
        values=list(fixture())
        # Keep 61 observations while introducing the same intermediate loss as
        # the REST/WebSocket transition: oldest observation is pushed back.
        values[1]=[row for row in values[1]
                   if not(row["symbol"]=="BTCUSDT" and row["open_ms"]==OPEN-20*60000)]
        values[1].append({"symbol":"BTCUSDT","open_ms":OPEN-61*60000})
        result=self.health(values)
        indexed={row["symbol"]:row for row in result["symbols"]}
        self.assertEqual(1,indexed["BTCUSDT"]["missing_minutes"])
        self.assertTrue(indexed["SOLUSDT"]["context_ready"])
        self.assertFalse(result["ready"])

    def test_sixty_candles_are_warmup_not_full_one_hour_context(self):
        values=list(fixture())
        values[1]=[row for row in values[1] if row["open_ms"]!=OPEN-60*60000]
        result=self.health(values)
        self.assertTrue(all("warmup" in row["reasons"] for row in result["symbols"]))
        self.assertFalse(result["ready"])

    def test_current_model_must_have_its_own_fresh_predictions(self):
        values=list(fixture())
        values[2][0]["model_version"]="previous-version"
        result=self.health(values)
        self.assertFalse(result["ready"])
        self.assertEqual(["missing"],result["prediction_series"][0]["reasons"])

    def test_one_fresh_asset_cannot_hide_stale_other_asset(self):
        values=list(fixture())
        values[0][1]["last_close_ms"]-=600000
        result=self.health(values)
        self.assertFalse(result["ready"])
        self.assertIn("stale",result["symbols"][1]["reasons"])

    def test_driver_heartbeat_and_inference_enabled_are_required(self):
        values=list(fixture())
        values[3]["heartbeat_ms"]=NOW-61000
        values[3]["inference_enabled"]=False
        result=self.health(values)
        codes={item["code"] for item in result["reasons"]}
        self.assertIn("stream_stale",codes)
        self.assertIn("inference_disabled",codes)

    def test_future_clock_market_is_not_healthy(self):
        values=list(fixture())
        values[0][0]["last_close_ms"]=NOW+60000
        result=self.health(values)
        self.assertFalse(result["ready"])

    def test_health_endpoint_returns_503_when_database_is_up_but_inference_degraded(self):
        handler=Handler.__new__(Handler)
        handler.path="/health"
        handler.wfile=io.BytesIO()
        responses=[]
        handler.send_response=responses.append
        handler.send_header=lambda *_:None
        handler.end_headers=lambda:None
        degraded={"ready":False,"status":"degraded","reasons":[{"code":"prediction_stale"}]}
        with patch("cryptopulse.exporter.product_health",return_value=degraded):
            handler.do_GET()
        self.assertEqual([503],responses)
        self.assertFalse(json.loads(handler.wfile.getvalue())["ready"])

class DashboardRegressionTests(unittest.TestCase):
    PANEL={"id":10,"title":"Alertas de mercado"}

    def test_zero_rows_is_not_a_usable_empty_state(self):
        with self.assertRaises(AssertionError):
            validate_panel_result(self.PANEL,{"frames":[{"data":{"values":[[]]}}]})

    def test_honest_no_alerts_row_passes_without_fabricating_alert(self):
        result=validate_panel_result(self.PANEL,{"frames":[{"data":{"values":[["Sin señales activas"]]}}]})
        self.assertEqual(1,result["rows"])

    def test_grafana_result_error_cannot_hide_behind_http_200(self):
        with self.assertRaises(AssertionError):
            validate_panel_result(self.PANEL,{"error":"column does not exist","status":400})

if __name__=="__main__":
    unittest.main()
