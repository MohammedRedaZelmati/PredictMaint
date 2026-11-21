import hashlib
import json
import math
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((ROOT / "contracts" / "event.schema.json").read_text(encoding="utf-8"))
FEATURE_CONTRACT = json.loads(
    (ROOT / "contracts" / "feature_columns.json").read_text(encoding="utf-8")
)
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())


def validate_event(event):
    """Apply JSON Schema and the cross-field/finite checks required by V1."""
    VALIDATOR.validate(event)
    expected_id = f"fd001:{event['source_split']}:{event['engine_id']}:{event['cycle']}"
    if event["event_id"] != expected_id:
        raise ValueError("event_id does not match split, engine_id and cycle")
    measurements = [f"op_setting_{index}" for index in range(1, 4)]
    measurements += [f"sensor_{index:02d}" for index in range(1, 22)]
    if not all(math.isfinite(event[name]) for name in measurements):
        raise ValueError("measurements must be finite")
    json.dumps(event, allow_nan=False)


def sample_event(split="train"):
    event = {
        "schema_version": 1,
        "dataset": "NASA-CMAPSS-FD001",
        "source_split": split,
        "run_id": "10000000-0000-4000-8000-000000000001",
        "event_id": f"fd001:{split}:1:100",
        "engine_id": 1,
        "cycle": 100,
        "event_ts": "2026-11-21T10:00:00Z",
        "published_at": "2026-11-21T17:00:00Z",
    }
    event.update({f"op_setting_{index}": 0.0 for index in range(1, 4)})
    event.update({f"sensor_{index:02d}": float(index) for index in range(1, 22)})
    return event


class EventContractTests(unittest.TestCase):
    def test_schema_is_valid_and_complete_example_passes(self):
        Draft202012Validator.check_schema(SCHEMA)
        validate_event(sample_event())

    def test_train_and_test_engine_ids_are_separate_namespaces(self):
        train = sample_event("train")
        test = sample_event("test")
        validate_event(train)
        validate_event(test)
        self.assertNotEqual(train["event_id"], test["event_id"])

    def test_missing_measurement_and_rul_are_rejected(self):
        missing = sample_event()
        del missing["sensor_21"]
        with self.assertRaises(Exception):
            validate_event(missing)
        leaked = sample_event()
        leaked["target_rul"] = 20
        with self.assertRaises(Exception):
            validate_event(leaked)

    def test_nonfinite_measurement_and_incorrect_event_id_are_rejected(self):
        for value in (math.nan, math.inf, -math.inf):
            event = sample_event()
            event["sensor_01"] = value
            with self.assertRaises(Exception):
                validate_event(event)
        event = sample_event()
        event["event_id"] = "fd001:train:1:99"
        with self.assertRaises(ValueError):
            validate_event(event)

    def test_bad_types_and_timestamps_are_rejected(self):
        for field, value in (("cycle", 0), ("engine_id", "1"), ("run_id", "bad"),
                             ("event_ts", "2026-11-21T10:00:00+01:00")):
            event = sample_event()
            event[field] = value
            with self.assertRaises(Exception):
                validate_event(event)

    def test_feature_order_and_canonical_hash(self):
        features = ["cycle", *(f"op_setting_{i}" for i in range(1, 4))]
        features += [f"sensor_{i:02d}" for i in range(1, 22)]
        self.assertEqual(FEATURE_CONTRACT, {"schema_version": 1, "features": features})
        canonical = json.dumps(features, ensure_ascii=False, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        self.assertEqual(len(digest), 64)
        print(f"feature_schema_hash={digest}")


if __name__ == "__main__":
    unittest.main()
