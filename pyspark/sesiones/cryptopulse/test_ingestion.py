"""Regression tests: missing intermediate candles, restart and atomic local state.

python -m unittest cryptopulse.test_ingestion -v
Uses mocked Kafka/REST. No events are published to the running market topics.
"""
import copy
import io
import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from .contract import make_event, validate
from .core import save_json
from .producer import MINUTE, ReconciledPublisher

BASE = 1767225600000  # 2026-01-01 UTC, safely in the past.


def candle(start):
    return [start,"100.00000000","101.00000000","99.00000000","100.50000000",
            "1.00000000",start+59999,"100.50000000",1,"0","0","0"]


class FakeSession:
    def __init__(self, missing=()):
        self.missing=set(missing)
        self.requests=[]

    def get(self, url, params=None, timeout=None):
        self.requests.append(dict(params))
        payload=[candle(start) for start in range(params["startTime"],params["endTime"]+1,MINUTE)
                 if start not in self.missing]
        class Response:
            def raise_for_status(self):
                pass
            def json(self):
                return payload
        return Response()


class FakeKafka:
    def __init__(self, fail_once=None):
        self.events=[]
        self.fail_once=fail_once

    def send(self, topic, key, value):
        parent=self
        class Future:
            def get(self, timeout):
                if parent.fail_once==value["open_ms"]:
                    parent.fail_once=None
                    raise RuntimeError("ACK failed")
                parent.events.append(copy.deepcopy(value))
        return Future()


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.silence=redirect_stdout(io.StringIO())
        self.silence.__enter__()

    def tearDown(self):
        self.silence.__exit__(None,None,None)

    def publisher(self, previous=None, missing=(), fail_once=None):
        kafka,rest=FakeKafka(fail_once),FakeSession(missing)
        publisher=ReconciledPublisher(kafka,rest,previous,clock=lambda:BASE+200*MINUTE)
        return publisher,kafka,rest

    def test_websocket_jump_then_rest_recovers_intermediate_minute_once(self):
        p,kafka,_=self.publisher()
        p.reconcile(BASE+180*MINUTE,symbols=("BTCUSDT",))
        self.assertEqual(p.info["rest_reconciled_open_ms"]["BTCUSDT"],BASE+179*MINUTE)
        p.publish(make_event("BTCUSDT",candle(BASE+181*MINUTE),"live","websocket"))
        self.assertEqual(p.info["rest_reconciled_open_ms"]["BTCUSDT"],BASE+179*MINUTE)
        p.reconcile(BASE+182*MINUTE,symbols=("BTCUSDT",))
        starts=[event["open_ms"] for event in kafka.events]
        self.assertEqual(starts.count(BASE+180*MINUTE),1)
        self.assertEqual(starts.count(BASE+181*MINUTE),1)
        self.assertEqual(set(starts),set(range(BASE,BASE+182*MINUTE,MINUTE)))
        before=len(kafka.events)
        p.reconcile(BASE+182*MINUTE,symbols=("BTCUSDT",))
        self.assertEqual(before,len(kafka.events))

    def test_legacy_state_does_not_hide_gap_below_highest_published(self):
        p,kafka,rest=self.publisher({"last_open_ms":{"BTCUSDT":BASE+181*MINUTE},"published":500})
        p.reconcile(BASE+182*MINUTE,symbols=("BTCUSDT",))
        self.assertIn(BASE+180*MINUTE,[event["open_ms"] for event in kafka.events])
        self.assertLessEqual(rest.requests[0]["startTime"],BASE+180*MINUTE)
        self.assertEqual(p.info["last_open_ms"]["BTCUSDT"],BASE+181*MINUTE)

    def test_restart_and_reconnect_preserve_independent_rest_cursor(self):
        p,_,_=self.publisher()
        p.reconcile(BASE+180*MINUTE,symbols=("BTCUSDT",))
        p.publish(make_event("BTCUSDT",candle(BASE+181*MINUTE),"live","websocket"))
        restarted,kafka,_=self.publisher(copy.deepcopy(p.info))
        restarted.reconcile(BASE+182*MINUTE,symbols=("BTCUSDT",))
        self.assertIn(BASE+180*MINUTE,[event["open_ms"] for event in kafka.events])
        self.assertEqual(restarted.info["rest_reconciled_open_ms"]["BTCUSDT"],BASE+181*MINUTE)

    def test_failed_kafka_ack_does_not_commit_rest_page(self):
        old_cursor=BASE+99*MINUTE
        p,kafka,_=self.publisher({"rest_reconciled_open_ms":{"BTCUSDT":old_cursor}},fail_once=BASE+120*MINUTE)
        with self.assertRaisesRegex(RuntimeError,"ACK failed"):
            p.reconcile(BASE+180*MINUTE,symbols=("BTCUSDT",))
        self.assertEqual(p.info["rest_reconciled_open_ms"]["BTCUSDT"],old_cursor)
        p.reconcile(BASE+180*MINUTE,symbols=("BTCUSDT",))
        self.assertEqual(p.info["rest_reconciled_open_ms"]["BTCUSDT"],BASE+179*MINUTE)
        starts=[event["open_ms"] for event in kafka.events]
        self.assertEqual(len(starts),len(set(starts)))

    def test_only_fully_closed_candles_at_snapshot_boundary(self):
        p,kafka,_=self.publisher()
        p.reconcile(BASE+180*MINUTE-1,symbols=("BTCUSDT",))
        self.assertEqual(max(event["open_ms"] for event in kafka.events),BASE+178*MINUTE)
        p.reconcile(BASE+180*MINUTE,symbols=("BTCUSDT",))
        self.assertEqual(max(event["open_ms"] for event in kafka.events),BASE+179*MINUTE)

    def test_temporarily_missing_source_candle_is_not_fabricated_and_is_revisited(self):
        missing=BASE+175*MINUTE
        p,kafka,rest=self.publisher(missing=(missing,))
        p.reconcile(BASE+180*MINUTE,symbols=("BTCUSDT",))
        self.assertNotIn(missing,[event["open_ms"] for event in kafka.events])
        self.assertEqual(p.info["source_gaps"]["BTCUSDT"]["count"],1)
        rest.missing.clear()
        p.reconcile(BASE+180*MINUTE,symbols=("BTCUSDT",))
        self.assertIn(missing,[event["open_ms"] for event in kafka.events])
        self.assertEqual(p.info["source_gaps"]["BTCUSDT"]["count"],0)


class ContractBoundaryTests(unittest.TestCase):
    def test_malformed_values_rejected_before_database(self):
        valid=make_event("BTCUSDT",candle(BASE),"replay","replay")
        cases=[None,[],{**valid,"schema_version":True},{**valid,"open_ms":True},
               {**valid,"number_of_trades":True},{**valid,"number_of_trades":2**63},
               {**valid,"volume":"1e30"},{**valid,"close":"NaN"},
               {**valid,"volume":"0.000000001"},{**valid,"open":"nonsense"}]
        for event in cases:
            with self.subTest(event=event),self.assertRaises((ValueError,KeyError)):
                validate(event)
        self.assertEqual(validate(valid),valid)


class AtomicStateTests(unittest.TestCase):
    def test_transient_windows_replace_lock_retries_and_cleans_temp(self):
        original=Path.replace
        attempts=[]
        def flaky(path,target):
            attempts.append(path)
            if len(attempts)<3:
                raise PermissionError("sharing violation")
            return original(path,target)
        with tempfile.TemporaryDirectory() as folder:
            target=Path(folder)/"state.json"
            with patch.object(Path,"replace",flaky),patch("cryptopulse.core.time.sleep"):
                save_json(target,{"complete":True})
            self.assertEqual(json.loads(target.read_text()),{"complete":True})
            self.assertEqual(len(attempts),3)
            self.assertEqual(list(Path(folder).glob("*.tmp")),[])

    def test_permanent_lock_preserves_previous_state(self):
        with tempfile.TemporaryDirectory() as folder:
            target=Path(folder)/"state.json"
            save_json(target,{"old":True})
            with patch.object(Path,"replace",side_effect=PermissionError("locked")),patch("cryptopulse.core.time.sleep"):
                with self.assertRaises(PermissionError):
                    save_json(target,{"new":True})
            self.assertEqual(json.loads(target.read_text()),{"old":True})
            self.assertEqual(list(Path(folder).glob("*.tmp")),[])

    def test_concurrent_writers_do_not_share_temporary_name(self):
        with tempfile.TemporaryDirectory() as folder:
            target=Path(folder)/"state.json"
            with ThreadPoolExecutor(max_workers=6) as executor:
                list(executor.map(lambda n:save_json(target,{"value":n,"payload":"x"*1000}),range(40)))
            value=json.loads(target.read_text())
            self.assertIn(value["value"],range(40))
            self.assertEqual(value["payload"],"x"*1000)
            self.assertEqual(list(Path(folder).glob("*.tmp")),[])


if __name__=="__main__":
    unittest.main()
