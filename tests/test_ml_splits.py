import unittest

from batch.train_models import EXPECTED_FEATURES, snapshot_cycles, split_engines


class MlSplitTests(unittest.TestCase):
    def test_grouped_split_is_disjoint_and_deterministic(self):
        fit, validation = split_engines(range(1, 101))
        self.assertEqual((len(fit), len(validation)), (80, 20))
        self.assertFalse(fit & validation)
        self.assertEqual(fit | validation, set(range(1, 101)))
        self.assertEqual((fit, validation), split_engines(reversed(range(1, 101))))

    def test_incomplete_or_duplicate_engines_fail(self):
        with self.assertRaises(ValueError):
            split_engines(range(1, 100))
        with self.assertRaises(ValueError):
            split_engines([*range(1, 101), 1])

    def test_snapshot_cycles_and_feature_exclusions(self):
        self.assertEqual(snapshot_cycles(200), (120, 150, 180))
        self.assertEqual(len(EXPECTED_FEATURES), 25)
        self.assertEqual(EXPECTED_FEATURES[:4], ["cycle", "op_setting_1", "op_setting_2", "op_setting_3"])
        self.assertEqual(EXPECTED_FEATURES[-1], "sensor_21")
        for excluded in ("engine_id", "source_split", "target_rul", "run_id", "event_id", "event_ts"):
            self.assertNotIn(excluded, EXPECTED_FEATURES)


if __name__ == "__main__":
    unittest.main()
