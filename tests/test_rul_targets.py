import unittest

from batch.prepare import rul_for_cycle


class RulTargetTests(unittest.TestCase):
    def test_terminal_and_cap(self):
        self.assertEqual(rul_for_cycle(200, 200), 0)
        self.assertEqual(rul_for_cycle(200, 199), 1)
        self.assertEqual(rul_for_cycle(200, 1), 125)

    def test_invalid_cycles(self):
        with self.assertRaises(ValueError):
            rul_for_cycle(100, 101)
        with self.assertRaises(ValueError):
            rul_for_cycle(100, 0)


if __name__ == "__main__":
    unittest.main()
