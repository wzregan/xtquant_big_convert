"""Skip rebuilding a payload that has nothing to rebuild.

_restore_jsonable walks the whole response looking for DataFrame/Series/Panel
envelopes. A whole-market snapshot has none: it is ~20 MB of plain scalars, and
walking it to rebuild nothing measured 345.9ms -- more than json.loads spent
parsing it in the first place.

Checking the raw text for the marker instead is a C substring scan over the
same bytes: 3.7ms. So the transport answers the question once, while it still
has the text, and the client skips the walk when the answer is no.

    current _restore_jsonable                     345.9ms
    identity-preserving walk (no transport change) 290.5ms   1.2x
    text-gated skip                                  3.7ms  93.8x

The 1.2x variant is here as a warning: the cost is the walk itself, not the
allocations it does along the way, so avoiding the rebuild buys almost nothing.

What must never happen is skipping a walk that was needed -- these tests are
mostly about that.
"""

import json
import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.redis_rpc import (
    TYPED_PAYLOAD_FLAG, TYPED_PAYLOAD_MARKER, loads_rpc_response, to_jsonable)
from bigqmt_signal_trader.xtquant_compat import BigQmtRpcClient, _restore_jsonable


FRAME = {"__bigqmt_type__": "DataFrame", "columns": ["a"], "records": [{"a": 1}]}
TICKS = {"600000.SH": {"lastPrice": 9.16, "askVol": [1, 2, 3]},
         "000001.SZ": {"lastPrice": 11.6, "askVol": [4, 5, 6]}}


def _envelope(data):
    return {"schema_version": 1, "request_id": "r", "data": data}


class MarkerDetectionTest(unittest.TestCase):
    """The transport's one job here: did the text contain an envelope?"""

    def _flag(self, data):
        raw = json.dumps(_envelope(data), ensure_ascii=False)
        return loads_rpc_response(raw)[TYPED_PAYLOAD_FLAG]

    def test_plain_ticks_carry_no_marker(self):
        self.assertFalse(self._flag(TICKS))

    def test_a_dataframe_envelope_is_seen(self):
        self.assertTrue(self._flag(FRAME))

    def test_one_envelope_buried_in_a_big_payload_is_seen(self):
        """The scan must not miss a needle: a single typed value among
        thousands of plain ones still has to be rebuilt."""
        data = dict(TICKS)
        for index in range(5000):
            data["%06d.SH" % index] = {"lastPrice": 1.0}
        data["buried"] = FRAME

        self.assertTrue(self._flag(data))

    def test_a_series_envelope_is_seen(self):
        self.assertTrue(self._flag({"__bigqmt_type__": "Series", "data": {"a": 1}}))

    def test_a_panel_envelope_is_seen(self):
        self.assertTrue(self._flag(
            {"__bigqmt_type__": "Panel", "items": ["x"], "data": {"x": FRAME}}))

    def test_the_marker_is_the_quoted_key(self):
        """Bare text mentioning the name must not trip it -- but a real key
        must, so the quotes matter in both directions."""
        self.assertEqual(TYPED_PAYLOAD_MARKER, '"__bigqmt_type__"')
        self.assertFalse(self._flag({"note": "mentions __bigqmt_type__ in prose"}))

    def test_a_non_dict_response_is_left_alone(self):
        self.assertEqual(loads_rpc_response(json.dumps([1, 2, 3])), [1, 2, 3])


class RestoreStillHappensTest(unittest.TestCase):
    """Skipping a walk that was needed would be silent data corruption."""

    def test_a_flagged_payload_is_rebuilt(self):
        import pandas as pd

        raw = json.dumps(_envelope(FRAME), ensure_ascii=False)
        response = loads_rpc_response(raw)
        self.assertTrue(response[TYPED_PAYLOAD_FLAG])

        self.assertIsInstance(_restore_jsonable(response["data"]), pd.DataFrame)

    def test_an_unflagged_payload_is_unchanged_by_the_walk_anyway(self):
        """The optimisation is only safe because the walk was a no-op here."""
        raw = json.dumps(_envelope(TICKS), ensure_ascii=False)
        response = loads_rpc_response(raw)

        self.assertFalse(response[TYPED_PAYLOAD_FLAG])
        self.assertEqual(_restore_jsonable(response["data"]), response["data"])

    def test_skipping_matches_walking_on_plain_data(self):
        """The two paths must be indistinguishable to a caller."""
        raw = json.dumps(_envelope(TICKS), ensure_ascii=False)
        data = loads_rpc_response(raw)["data"]

        self.assertEqual(data, _restore_jsonable(data))


class FlagHygieneTest(unittest.TestCase):
    def test_the_flag_never_lands_inside_data(self):
        """It rides on the envelope; leaking into the payload would show up as
        a phantom instrument in a snapshot."""
        raw = json.dumps(_envelope(TICKS), ensure_ascii=False)
        response = loads_rpc_response(raw)

        self.assertNotIn(TYPED_PAYLOAD_FLAG, response["data"])

    def test_the_client_pops_it(self):
        """实际验证标志不泄露、普通行情跳过重建；不依赖源码中分支的写法。"""
        for flag in (False, True, None):
            with self.subTest(flag=flag):
                response = {"ok": True, "data": TICKS}
                if flag is not None:
                    response[TYPED_PAYLOAD_FLAG] = flag
                client = BigQmtRpcClient(account_id="acct")
                client._transport_instance = SimpleNamespace(send_request=Mock(return_value=response))
                with patch("bigqmt_signal_trader.xtquant_compat._restore_jsonable",
                           wraps=_restore_jsonable) as restore:
                    data = client.call("get_full_tick", use_formula=False)
                    self.assertEqual(data, TICKS)
                    self.assertNotIn(TYPED_PAYLOAD_FLAG, data)
                    self.assertNotIn(TYPED_PAYLOAD_FLAG, response)
                    if flag is False:
                        restore.assert_not_called()
                    else:
                        restore.assert_called()

    def test_an_absent_flag_means_walk(self):
        """In-process routing never sees the text. Assuming "nothing to do"
        there would skip a rebuild that was needed."""
        import io
        path = os.path.join(ROOT, "src", "bigqmt_signal_trader", "transports",
                            "zmq_transport.py")
        with io.open(path, encoding="utf-8") as handle:
            source = handle.read()
        start = source.index("def _loads(raw):")
        end = source.index("\nclass ", start)
        body = source[start:end]

        # The already-a-dict branch returns before any flag is set.
        self.assertIn("return dict(raw)", body)
        self.assertLess(body.index("return dict(raw)"),
                        body.index("TYPED_PAYLOAD_FLAG"))


class ServerSideCostTest(unittest.TestCase):
    """to_jsonable is the other half, and it is NOT skippable the same way."""

    def test_it_still_converts_nan_to_none(self):
        """json.dumps alone writes a bare NaN literal; the conversion here is
        real work, not waste, which is why this half was left alone."""
        self.assertIsNone(to_jsonable(float("nan")))
        self.assertIsNone(to_jsonable(float("inf")))

    def test_it_still_builds_a_dataframe_envelope(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")

        payload = to_jsonable(pd.DataFrame([{"a": 1}]))

        self.assertEqual(payload["__bigqmt_type__"], "DataFrame")


if __name__ == "__main__":
    unittest.main()
