from __future__ import annotations

import unittest

from pscripts.refresh_regulations_database import (
    BBox,
    bbox_overlap,
    extract_bbox_from_geometry_columns,
    extract_bbox_from_numeric_columns,
    mark_missing_rule_versions,
    normalize_bbox,
    primary_citation,
    rule_version_fingerprint,
    resolve_rule_zone_bbox,
    safe_chunk_index,
    to_spot_items,
    to_zone_items,
    upsert_source_candidates,
)


class _FakeResponse:
    data = [{"id": "ok"}]


class _FakeTable:
    def __init__(self) -> None:
        self.rows = []

    def upsert(self, data, on_conflict=None):
        self.rows.append((data, on_conflict))
        return self

    def execute(self):
        return _FakeResponse()


class _FakeClient:
    def __init__(self) -> None:
        self.tables = {}

    def table(self, name):
        table = self.tables.setdefault(name, _FakeTable())
        return table


class _FakeVersionResponse:
    def __init__(self, data):
        self.data = data


class _FakeVersionTable:
    def __init__(self, current_rows):
        self.current_rows = current_rows
        self.updated_payload = None
        self.updated_ids = None

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def update(self, payload):
        self.updated_payload = payload
        return self

    def in_(self, _column, values):
        self.updated_ids = values
        return self

    def execute(self):
        if self.updated_payload is not None:
            return _FakeVersionResponse([])
        return _FakeVersionResponse(self.current_rows)


class _FakeVersionClient:
    def __init__(self, current_rows):
        self.version_table = _FakeVersionTable(current_rows)

    def table(self, name):
        self.assert_name = name
        return self.version_table


class RefreshRegulationsDatabaseTests(unittest.TestCase):
    def test_legacy_sync_module_still_exports_main(self) -> None:
        from pscripts import sync_regulation_geospatial_links as legacy_module

        self.assertTrue(callable(legacy_module.main))

    def test_upsert_source_candidates_records_discovery_metadata(self) -> None:
        client = _FakeClient()

        count = upsert_source_candidates(
            client,
            [
                {
                    "id": "discovered_abc",
                    "url": "https://example.gouv.fr/reglementation-peche-de-loisir-2027.html",
                    "title": "Reglementation peche de loisir 2027",
                    "kind": "html+pdf+links",
                    "source_type": "DIRM",
                    "authority_name": "DIRM",
                    "status": "auto_accepted",
                    "discovery_score": 22,
                    "matched_keywords": ["reglementation"],
                }
            ],
            "2026-05-01T00:00:00+00:00",
        )

        self.assertEqual(count, 1)
        row, on_conflict = client.tables["reg_source_candidates"].rows[0]
        self.assertEqual(on_conflict, "candidate_key")
        self.assertEqual(row["candidate_key"], "discovered_abc")
        self.assertEqual(row["status"], "auto_accepted")

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

    def test_to_spot_items_does_not_keep_zone_link(self) -> None:
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
        self.assertNotIn("zone_id", items[0])

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

    def test_rule_version_fingerprint_changes_with_content(self) -> None:
        base_rule = {
            "rule_key": "species.bar.min-size.namo",
            "rule_type": "MIN_SIZE",
            "title": "Taille minimale bar",
            "description": "Bar commun : 42 cm",
            "metric_type": "SIZE_MIN_CM",
            "metric_value": 42,
            "metric_unit": "cm",
            "species_common_name": "bar",
            "zone": {"zone_code": "FACADE_NAMO"},
        }
        changed_rule = dict(base_rule, metric_value=45)

        self.assertNotEqual(rule_version_fingerprint(base_rule), rule_version_fingerprint(changed_rule))

    def test_mark_missing_rule_versions_closes_unseen_current_versions(self) -> None:
        client = _FakeVersionClient(
            [
                {"id": "v1", "rule_key": "rule.seen"},
                {"id": "v2", "rule_key": "rule.old"},
            ]
        )

        count = mark_missing_rule_versions(
            client,
            {"rule.seen"},
            "2026-05-01T00:00:00+00:00",
            "run-1",
        )

        self.assertEqual(count, 1)
        self.assertEqual(client.version_table.updated_ids, ["v2"])
        self.assertEqual(client.version_table.updated_payload["status"], "possibly_removed")
        self.assertFalse(client.version_table.updated_payload["is_current"])

    def test_safe_chunk_index_stays_in_postgres_integer_range(self) -> None:
        value = safe_chunk_index("species.bar.declaration.mediterranee")

        self.assertGreaterEqual(value, 1)
        self.assertLessEqual(value, 2_147_483_647)


if __name__ == "__main__":
    unittest.main()
