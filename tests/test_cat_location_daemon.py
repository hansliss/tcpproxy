#!/usr/bin/env python3
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.cat_location_daemon import (
    LocationResolver,
    LocationPublisher,
    parse_tracker_payload,
)

KML_PATH = Path(__file__).resolve().parent.parent / "Locations.kml"


class TestCatLocationDaemon(unittest.TestCase):
    def test_parse_tracker_payload(self):
        payload = "[SG*1234567890*0066*UD,270524,061232,V,9.999851,N,30.000207,W,0.0,176,11,00,80,99,0,50,00000000,1,1,240,1,36,57745184,22,,00]"
        result = parse_tracker_payload(payload)
        self.assertIsNotNone(result)
        device, lat, lon = result
        self.assertEqual(device, "1234567890")
        self.assertAlmostEqual(lat, 9.999851)
        self.assertAlmostEqual(lon, -30.000207)

    def test_location_resolver_nearest(self):
        resolver = LocationResolver(KML_PATH)
        # Coordinates close to "in front of the house"
        lat = 10.00005
        lon = -30.00030
        location = resolver.resolve(lat, lon)
        self.assertEqual(location, "in front of the house")

    def test_publisher_emits_on_change_only(self):
        resolver = LocationResolver(KML_PATH)
        emitted = []

        def collector(event):
            emitted.append(event)

        publisher = LocationPublisher(resolver, collector)
        event = {
            "timestamp": "2024-05-27 08:11:20",
            "payload": "[SG*1234567890*0066*UD,270524,061232,V,9.999851,N,30.000207,W,0.0,176,11,00,80,99,0,50,00000000,1,1,240,1,36,57745184,22,,00]",
        }
        publisher.process_event(event)
        # Second event with same coordinates should not emit
        publisher.process_event(event)
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0]["position"], "at the back of the house")
        self.assertEqual(emitted[0]["tracker_id"], "1234567890")

        # New location
        event2 = {
            "timestamp": "2024-05-27 09:00:00",
            "payload": "[SG*1234567890*0066*UD,270524,061232,V,9.999050,N,29.999356,W,0.0,176,11,00,80,99,0,50,00000000,1,1,240,1,36,57745184,22,,00]",
        }
        publisher.process_event(event2)
        self.assertEqual(len(emitted), 2)
        self.assertEqual(emitted[-1]["position"], "in the field to the south")
        self.assertEqual(emitted[-1]["tracker_id"], "1234567890")


if __name__ == "__main__":
    unittest.main()
