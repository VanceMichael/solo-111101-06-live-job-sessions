import unittest
from collector_06.adapter import decode


class CalendarWireTests(unittest.TestCase):
    def test_folded_properties_and_recurrence_identity(self):
        content = b"BEGIN:VEVENT\r\nUID:A\r\nRECURRENCE-ID:20261111T010000Z\r\nSUMMARY:hello\r\n world\r\nEND:VEVENT\r\n"
        event = decode(content)[0]
        self.assertEqual(event["SUMMARY"], "helloworld")
        self.assertEqual(event["RECURRENCE-ID"], "20261111T010000Z")

    def test_cancel_without_time_is_not_dropped(self):
        rows = decode(b"BEGIN:VEVENT\nUID:A\nSEQUENCE:3\nSTATUS:CANCELLED\nEND:VEVENT\n")
        self.assertEqual(rows[0]["STATUS"], "CANCELLED")
