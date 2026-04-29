from __future__ import annotations

import unittest

from pscripts.sync_regulation_geospatial_links import (
    BBox,
    bbox_overlap,
    extract_bbox_from_geometry_columns,
    extract_bbox_from_numeric_columns,
    normalize_bbox,
    primary_citation,
    resolve_rule_zone_bbox,
    safe_chunk_index,
    to_spot_items,
    to_zone_items,
)


class SyncRegulationGeospatialLinksTests(unittest.TestCase):
    def test_extract_bbox_from_numeric_columns_spot(self) -> None:
        row = {
            "id": "s1",
            "latitude_min": 48.60,
            "latitude_max": 48.62,
            "longitude_min": -4.65,
            "longitude_max": -4.60,
        }

        bbox = extract_bbox_from_numeric_columns(row)

        self.assertIsNotNone(bbox)
        self.assertEqual(bbox, BBox(lat_min=48.60, lat_max=48.62, lon_min=-4.65, lon_max=-4.60))

    def test_extract_bbox_from_geometry_columns_zone_polygon_geojson(self) -> None:
        row = {
            "id": "z1",
            "polygon": {
                "type": "Polygon",
                "coordinates": [
                    [[-4.70, 48.50], [-4.50, 48.50], [-4.50, 48.70], [-4.70, 48.70], [-4.70, 48.50]]
                ],
            },
        }

        bbox = extract_bbox_from_geometry_columns(row)

        self.assertIsNotNone(bbox)
        self.assertEqual(bbox, BBox(lat_min=48.50, lat_max=48.70, lon_min=-4.70, lon_max=-4.50))

    def test_extract_bbox_from_geometry_columns_feature_collection(self) -> None:
        row = {
            "id": "z2",
            "polygon": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [[-4.8, 48.3], [-4.6, 48.3], [-4.6, 48.5], [-4.8, 48.5], [-4.8, 48.3]]
                            ],
                        },
                    }
                ],
            },
        }

        bbox = extract_bbox_from_geometry_columns(row)

        self.assertIsNotNone(bbox)
        self.assertEqual(bbox, BBox(lat_min=48.3, lat_max=48.5, lon_min=-4.8, lon_max=-4.6))

    def test_extract_bbox_from_geometry_columns_list_coordinates(self) -> None:
        row = {
            "id": "z3",
            "polygon": [[[-4.4, 47.9], [-4.2, 47.9], [-4.2, 48.1], [-4.4, 48.1], [-4.4, 47.9]]],
        }

        bbox = extract_bbox_from_geometry_columns(row)

        self.assertIsNotNone(bbox)
        self.assertEqual(bbox, BBox(lat_min=47.9, lat_max=48.1, lon_min=-4.4, lon_max=-4.2))

    def test_bbox_overlap(self) -> None:
        a = normalize_bbox(48.0, 49.0, -5.0, -4.0)
        b = normalize_bbox(48.5, 49.5, -4.5, -3.5)
        c = normalize_bbox(50.0, 51.0, -3.0, -2.0)

        self.assertTrue(bbox_overlap(a, b))
        self.assertFalse(bbox_overlap(a, c))

    def test_to_spot_items_keeps_zone_link(self) -> None:
        rows = [
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "name": "Spot A",
                "zone_id": "22222222-2222-2222-2222-222222222222",
                "latitude_min": 48.0,
                "latitude_max": 48.1,
                "longitude_min": -4.2,
                "longitude_max": -4.1,
            },
            {
                "id": "missing-bbox",
                "name": "Spot B",
                "zone_id": "33333333-3333-3333-3333-333333333333",
            },
        ]

        items = to_spot_items(rows)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["id"], "11111111-1111-1111-1111-111111111111")
        self.assertEqual(items[0]["zone_id"], "22222222-2222-2222-2222-222222222222")

    def test_to_zone_items_from_polygon(self) -> None:
        rows = [
            {
                "id": "aaaaaaa1-aaaa-aaaa-aaaa-aaaaaaaaaaa1",
                "name": "Zone A",
                "polygon": {
                    "type": "Polygon",
                    "coordinates": [
                        [[-4.3, 47.9], [-4.1, 47.9], [-4.1, 48.1], [-4.3, 48.1], [-4.3, 47.9]]
                    ],
                },
            }
        ]

        items = to_zone_items(rows)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["id"], "aaaaaaa1-aaaa-aaaa-aaaa-aaaaaaaaaaa1")
        self.assertEqual(items[0]["name"], "Zone A")

    def test_resolve_rule_zone_bbox_strategies(self) -> None:
        spots_envelope = BBox(lat_min=47.0, lat_max=49.0, lon_min=-5.0, lon_max=-3.0)
        zones_envelope = BBox(lat_min=47.5, lat_max=48.5, lon_min=-4.5, lon_max=-3.5)

        rule_spots = {
            "rule_key": "r1",
            "zone": {"strategy": "SPOTS_ENVELOPE"},
        }
        rule_zones = {
            "rule_key": "r2",
            "zone": {"strategy": "APP_ZONES_UNION"},
        }
        rule_custom = {
            "rule_key": "r3",
            "zone": {
                "strategy": "CUSTOM_BBOX",
                "lat_min": 43.0,
                "lat_max": 44.0,
                "lon_min": 5.0,
                "lon_max": 6.0,
            },
        }

        self.assertEqual(resolve_rule_zone_bbox(rule_spots, spots_envelope, zones_envelope), spots_envelope)
        self.assertEqual(resolve_rule_zone_bbox(rule_zones, spots_envelope, zones_envelope), zones_envelope)
        self.assertEqual(
            resolve_rule_zone_bbox(rule_custom, spots_envelope, zones_envelope),
            BBox(lat_min=43.0, lat_max=44.0, lon_min=5.0, lon_max=6.0),
        )

    def test_resolve_rule_zone_bbox_fallbacks_to_spots_when_zones_missing(self) -> None:
        spots_envelope = BBox(lat_min=47.0, lat_max=49.0, lon_min=-5.0, lon_max=-3.0)
        rule_zones = {
            "rule_key": "r4",
            "zone": {"strategy": "APP_ZONES_UNION"},
        }

        self.assertEqual(resolve_rule_zone_bbox(rule_zones, spots_envelope, None), spots_envelope)

    def test_primary_citation_prefers_embedded_citation(self) -> None:
        rule = {
            "rule_key": "r1",
            "description": "fallback",
            "source": {"source_url": "https://example.test/fallback"},
            "citations": [
                {
                    "source_url": "https://example.test/doc.pdf",
                    "quote": "preuve",
                    "document_hash": "abc",
                }
            ],
        }

        citation = primary_citation(rule)

        self.assertEqual(citation["source_url"], "https://example.test/doc.pdf")
        self.assertEqual(citation["document_hash"], "abc")

    def test_primary_citation_builds_legacy_fallback(self) -> None:
        rule = {
            "rule_key": "r1",
            "description": "fallback",
            "source": {"source_url": "https://example.test/fallback", "title": "Doc"},
        }

        citation = primary_citation(rule)

        self.assertEqual(citation["quote"], "fallback")
        self.assertEqual(citation["source_url"], "https://example.test/fallback")

    def test_safe_chunk_index_stays_in_postgres_integer_range(self) -> None:
        value = safe_chunk_index("species.bar.declaration.mediterranee")

        self.assertGreaterEqual(value, 1)
        self.assertLessEqual(value, 2_147_483_647)


if __name__ == "__main__":
    unittest.main()
