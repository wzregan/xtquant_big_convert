"""to_jsonable's fast path must be a speed change and nothing else.

A whole-market snapshot is 1.9M nodes, and every one of them used to walk the
full type-probing chain -- starting with a getattr(value, "item") that misses
on every plain float. Checking the handful of types that dominate the payload
first cuts it roughly in half:

    26744 instruments   346.3ms -> 164.9ms
    51285 instruments   669.4ms -> 345.8ms

That time is held on the QMT listener thread with the GIL, so removing it also
removes GIL pressure the whole bridge was paying.

The checks are by exact type rather than isinstance, and that is the whole
safety argument: numpy's float64 subclasses float, so an isinstance check
would swallow it into the fast path and return it unconverted. Identity checks
let it fall through to _maybe_scalar, which unwraps it as before. These tests
exist to keep that true.
"""

import datetime as dt
import json
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.redis_rpc import to_jsonable


class ScalarTest(unittest.TestCase):
    def test_nan_becomes_none(self):
        """json.dumps would write a bare NaN literal; callers expect None."""
        self.assertIsNone(to_jsonable(float("nan")))

    def test_infinities_become_none(self):
        self.assertIsNone(to_jsonable(float("inf")))
        self.assertIsNone(to_jsonable(float("-inf")))

    def test_ordinary_scalars_pass_through(self):
        for value in (9.16, 577464, "20260831", True, False, None):
            self.assertEqual(to_jsonable(value), value, repr(value))

    def test_bool_stays_a_bool(self):
        """bool subclasses int; the fast path must not flatten it."""
        self.assertIs(to_jsonable(True), True)
        self.assertIsInstance(to_jsonable(False), bool)


class ContainerTest(unittest.TestCase):
    def test_nested_nan_is_still_converted(self):
        self.assertEqual(to_jsonable({"a": [1.0, float("nan")]}), {"a": [1.0, None]})

    def test_dict_keys_become_strings(self):
        self.assertEqual(to_jsonable({1: "x"}), {"1": "x"})

    def test_tuples_and_sets_become_lists(self):
        """Neither is on the fast path, so they take the original branch."""
        self.assertEqual(to_jsonable((1, 2)), [1, 2])
        self.assertEqual(to_jsonable({7}), [7])

    def test_a_deep_payload_is_json_serialisable(self):
        snapshot = {"600000.SH": {"lastPrice": 9.16, "askPrice": [9.17, 9.18],
                                  "bad": float("nan")}}

        self.assertEqual(json.loads(json.dumps(to_jsonable(snapshot))),
                         {"600000.SH": {"lastPrice": 9.16,
                                        "askPrice": [9.17, 9.18], "bad": None}})


class NumpyTest(unittest.TestCase):
    """The reason the fast path checks exact types.

    np.float64 IS a float subclass. An isinstance-based fast path would return
    it untouched and it would reach json.dumps as a numpy object.
    """

    def setUp(self):
        try:
            import numpy  # noqa: F401
        except ImportError:
            self.skipTest("numpy not installed")

    def test_float64_is_unwrapped_to_a_python_float(self):
        import numpy as np

        result = to_jsonable(np.float64(9.16))

        self.assertEqual(result, 9.16)
        self.assertIs(type(result), float)

    def test_int64_is_unwrapped_to_a_python_int(self):
        import numpy as np

        result = to_jsonable(np.int64(5))

        self.assertEqual(result, 5)
        self.assertIs(type(result), int)

    def test_a_numpy_nan_still_becomes_none(self):
        import numpy as np

        self.assertIsNone(to_jsonable(np.float64("nan")))

    def test_they_survive_inside_a_container(self):
        """The fast path recurses; the values inside must take the slow one."""
        import numpy as np

        result = to_jsonable({"a": [np.float64(1.5), np.int64(2)]})

        self.assertEqual(result, {"a": [1.5, 2]})
        self.assertIs(type(result["a"][0]), float)


class RicherObjectsTest(unittest.TestCase):
    """Anything not a plain type must still reach the original handling."""

    def test_datetime_still_formats(self):
        self.assertEqual(to_jsonable(dt.datetime(2026, 8, 31, 11, 12, 0)),
                         "2026-08-31 11:12:00")

    def test_a_dataframe_still_becomes_an_envelope(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")

        payload = to_jsonable(pd.DataFrame([{"a": 1}]))

        self.assertEqual(payload["__bigqmt_type__"], "DataFrame")

    def test_an_object_still_serialises_by_its_attributes(self):
        class Order(object):
            def __init__(self):
                self.code = "600000.SH"
                self._private = "hidden"

        self.assertEqual(to_jsonable(Order()), {"code": "600000.SH"})


class ExactTypeGuardTest(unittest.TestCase):
    """Pin the mechanism, not just the outcome."""

    def test_the_fast_path_uses_identity_not_isinstance(self):
        import io

        path = os.path.join(ROOT, "src", "bigqmt_signal_trader", "redis_rpc.py")
        with io.open(path, encoding="utf-8") as handle:
            source = handle.read()
        start = source.index("def to_jsonable(value")
        body = source[start:start + 1400]

        self.assertIn("kind = type(value)", body)
        self.assertIn("kind is float", body)

    def test_a_float_subclass_is_not_taken_by_the_fast_path(self):
        """Stand-in for np.float64 when numpy is absent."""
        class Weird(float):
            def item(self):
                return 42.0

        self.assertEqual(to_jsonable(Weird(1.0)), 42.0)


if __name__ == "__main__":
    unittest.main()
