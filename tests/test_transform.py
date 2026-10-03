"""Unit tests for the transform step (no Kafka or MySQL needed).

Run from the repository root:
    python3 -m unittest discover -s tests -v
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "enhanced"))

from transform import InvalidMessage, parse_message  # noqa: E402


class ParseMessageTests(unittest.TestCase):

    def test_valid_message_bytes(self):
        row = parse_message(b"Fri Oct 16 19:26:20 2026,9820660,truck,4004")
        self.assertEqual(row, ("2026-10-16 19:26:20", 9820660, "truck", 4004))

    def test_ctime_double_space_single_digit_day(self):
        # time.ctime() pads single-digit days with an extra space
        row = parse_message(b"Fri Oct  2 19:26:20 2026,3623948,car,4008")
        self.assertEqual(row[0], "2026-10-02 19:26:20")

    def test_accepts_str_and_strips_whitespace(self):
        row = parse_message("Fri Oct  2 19:26:22 2026, 1059601 , van ,4001\n")
        self.assertEqual(row, ("2026-10-02 19:26:22", 1059601, "van", 4001))

    def test_wrong_field_count(self):
        with self.assertRaises(InvalidMessage):
            parse_message(b"Fri Oct  2 19:26:20 2026,9820660,truck")

    def test_bad_timestamp(self):
        with self.assertRaises(InvalidMessage):
            parse_message(b"2026-10-02 19:26:20,9820660,truck,4004")

    def test_unknown_vehicle_type(self):
        with self.assertRaises(InvalidMessage):
            parse_message(b"Fri Oct  2 19:26:20 2026,9820660,bicycle,4004")

    def test_non_numeric_id(self):
        with self.assertRaises(InvalidMessage):
            parse_message(b"Fri Oct  2 19:26:20 2026,abc,car,4004")

    def test_invalid_utf8(self):
        with self.assertRaises(InvalidMessage):
            parse_message(b"\xff\xfe\xfa")


if __name__ == "__main__":
    unittest.main()
