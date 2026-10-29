"""Invalid Kafka messages must not enter event-time windows or crash rejection."""
import json
import unittest
from .contract import make_event
from .stream import valid_market_payload

class WindowContractTests(unittest.TestCase):
    def setUp(self):
        self.event=make_event('BTCUSDT',[1793232420000,'81331.21','81349.67','81331.21','81344','2.38339',1793232479999,'193870.66905170',946],'replay','replay')

    def test_real_closed_candle_passes_same_gate(self):
        self.assertTrue(valid_market_payload(json.dumps(self.event)))

    def test_tombstone_malformed_json_and_non_object_are_rejected(self):
        for raw in (None,'{bad','null','[]','17'):
            with self.subTest(raw=raw):
                self.assertFalse(valid_market_payload(raw))

    def test_incomplete_or_nonfinite_prices_never_enter_windows(self):
        for field,value in [('open',None),('low','Infinity'),('high','NaN'),('volume','Infinity'),('quote_asset_volume','-1'),('source','other')]:
            with self.subTest(field=field,value=value):
                self.assertFalse(valid_market_payload(json.dumps({**self.event,field:value})))

if __name__=='__main__': unittest.main()
