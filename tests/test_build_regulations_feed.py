from __future__ import annotations

import unittest
from unittest.mock import patch

from pscripts.regulations.build_regulations_feed import (
    SourceRecord,
    ai_base_url_is_local,
    ai_base_url_is_openrouter,
    ai_request_headers,
    apply_ai_audit_to_rules,
    ai_message_content_to_text,
    build_base_rule,
    build_ai_request_payload,
    build_rule_candidates,
    build_source_documents_manifest,
    build_quality_report,
    deduplicate_rules,
    enrich_rule_for_publication,
    extract_first_json_object,
    extract_dirm_closure_rules,
    extract_dirm_protected_species_rules,
    extract_dirm_quota_rules,
    extract_dirm_size_rules,
    extract_pdf_urls_from_html,
    extract_pdf_url_from_html,
    extract_practice_restriction_rules,
    extract_relevant_html_links_from_html,
    extract_sensitive_species_declaration_rules,
    find_sentence,
    html_to_text,
    infer_rule_validity,
    parse_legifrance_diving_rules,
    parse_legifrance_spearfishing_rules,
    parse_ai_response_payload,
    parse_french_date_to_iso,
    parse_ministere_spearfishing_rules,
    normalize_ai_confidence_scores,
    quote_matches_source_context,
    resolve_rule_zone,
    run_ai_rule_audit,
    source_context_window,
    should_try_pdf_ocr,
    summarize_ai_response_payload,
    validate_rule_set,
)


class BuildRegulationsFeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.legifrance_source = SourceRecord(
            source_id="legifrance_spearfishing",
            source_type="LEGIFRANCE",
            source_priority=1,
            authority_name="Legifrance",
            title="Article R921-90",
            url="https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000029978119",
            kind="html",
        )

        self.dirm_source = SourceRecord(
            source_id="dirm_namo_capture_rules",
            source_type="DIRM",
            source_priority=2,
            authority_name="DIRM",
            title="DIRM regles peche",
            url="https://www.dirm.example/page.html",
            kind="html+pdf",
        )

    def test_extract_pdf_url_from_html_prefers_capture_pdf(self) -> None:
        html = """
        <a href="/docs/autre.pdf">Autre doc</a>
        <a href="/IMG/pdf/taille_minimale_capture_2026.pdf">Tailles minimales</a>
        """

        url = extract_pdf_url_from_html(html, "https://www.dirm.example/page.html")

        self.assertEqual(url, "https://www.dirm.example/IMG/pdf/taille_minimale_capture_2026.pdf")

    def test_extract_pdf_urls_from_html_uses_anchor_text_for_ranking(self) -> None:
        html = """
        <a href="/docs/z9.pdf">Archive interne</a>
        <a href="/docs/a1.pdf">Reglementation peche de loisir - tailles minimales</a>
        """

        urls = extract_pdf_urls_from_html(html, "https://www.dirm.example/page.html", limit=2)

        self.assertEqual(urls[0], "https://www.dirm.example/docs/a1.pdf")

    def test_parse_legifrance_spearfishing_rules(self) -> None:
        html = """
        <p>L'exercice de la peche sous-marine au moyen d'un fusil-harpon est interdit aux personnes agees de moins de seize ans.</p>
        """

        rules = parse_legifrance_spearfishing_rules(self.legifrance_source, html)

        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0]["rule_type"], "SPEARFISHING_GENERAL")
        self.assertEqual(rules[0]["metric_type"], "AGE_MIN_YEARS")
        self.assertEqual(rules[0]["metric_value"], 16)

    def test_parse_legifrance_diving_rules(self) -> None:
        html = """
        <p>Les dispositions de la presente section s'appliquent aux etablissements mentionnes a l'article L. 322-2 qui organisent la pratique de la plongee subaquatique.</p>
        """
        source = SourceRecord(
            source_id="legifrance_diving",
            source_type="LEGIFRANCE",
            source_priority=1,
            authority_name="Legifrance",
            title="Article A322-71",
            url="https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000025393881",
            kind="html",
        )

        rules = parse_legifrance_diving_rules(source, html)

        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0]["rule_type"], "DIVING_GENERAL")

    def test_parse_ministere_spearfishing_rules(self) -> None:
        source = SourceRecord(
            source_id="ministere_peche_loisir",
            source_type="MINISTERE_MER",
            source_priority=2,
            authority_name="Ministere de la Mer",
            title="Peche de loisir",
            url="https://www.mer.gouv.fr/peche-de-loisir-en-mer",
            kind="html",
        )

        html = """
        <li>Ne pas utiliser d'equipement respiratoire ;</li>
        <li>Ne pas pratiquer la peche sous-marine la nuit ;</li>
        <li>Avoir plus de 16 ans et une bonne condition physique</li>
        """

        rules = parse_ministere_spearfishing_rules(source, html)

        self.assertGreaterEqual(len(rules), 3)
        self.assertTrue(all(rule["rule_type"] == "SPEARFISHING_GENERAL" for rule in rules))
        self.assertTrue(all(rule["needs_manual_review"] for rule in rules))

    def test_extract_dirm_size_and_quota_rules(self) -> None:
        pdf_text = """
        Bar commun (Dicentrarchus labrax) : 42 cm
        Homard : 3 captures par jour
        Araignee de mer : 5 kg par jour
        """

        size_rules = extract_dirm_size_rules(self.dirm_source, "https://www.dirm.example/rules.pdf", pdf_text)
        quota_rules = extract_dirm_quota_rules(self.dirm_source, "https://www.dirm.example/rules.pdf", pdf_text)

        self.assertEqual(len(size_rules), 1)
        self.assertEqual(size_rules[0]["rule_type"], "MIN_SIZE")

        self.assertEqual(len(quota_rules), 2)
        self.assertTrue(all(rule["rule_type"] == "QUOTA" for rule in quota_rules))

    def test_extract_dirm_quota_rules_narrative_sentence(self) -> None:
        text = "La peche sous-marine des araignees est limitee a six unites par pecheur et par jour."

        quota_rules = extract_dirm_quota_rules(self.dirm_source, "https://www.dirm.example/rules.pdf", text)

        self.assertEqual(len(quota_rules), 1)
        self.assertEqual(quota_rules[0]["metric_type"], "QUOTA_MAX_UNITS")
        self.assertEqual(quota_rules[0]["metric_value"], 6.0)

    def test_extract_dirm_quota_rules_table_like_rows(self) -> None:
        text = """
        Quantite maxi de peche autorisee par pecheur et par jour
        Araignee de mer 5 unites
        """

        quota_rules = extract_dirm_quota_rules(self.dirm_source, "https://www.dirm.example/rules.pdf", text)

        self.assertEqual(len(quota_rules), 1)
        self.assertEqual(quota_rules[0]["species_common_name"], "araignees")
        self.assertEqual(quota_rules[0]["metric_value"], 5.0)

    def test_extract_dirm_closure_rules(self) -> None:
        text = "Aucun specimen de lieu jaune ne peut etre capture et detenu du 1er janvier au 30 avril."

        closure_rules = extract_dirm_closure_rules(self.dirm_source, "https://www.dirm.example/rules.pdf", text)

        self.assertEqual(len(closure_rules), 1)
        self.assertEqual(closure_rules[0]["rule_type"], "CLOSURE_PERIOD")
        self.assertEqual(closure_rules[0]["species_common_name"], "lieu jaune")

    def test_extract_dirm_protected_species_rules_multiple_species(self) -> None:
        text = "La peche du merou et du corb est interdite pour les pecheurs de loisir."

        protected_rules = extract_dirm_protected_species_rules(
            self.dirm_source, "https://www.dirm.example/rules.pdf", text
        )

        protected_species = sorted(rule["species_common_name"] for rule in protected_rules)
        self.assertEqual(protected_species, ["corb", "merou"])

    def test_extract_sensitive_species_declaration_rules(self) -> None:
        text = """
        Especes sensibles : Bar, Lieu jaune, Dorade rose.
        Les pecheurs de loisir doivent s'enregistrer et faire une declaration via RecFishing.
        """

        declaration_rules = extract_sensitive_species_declaration_rules(
            self.dirm_source,
            "https://www.dirm.example/rules.html",
            text,
        )

        species = sorted(rule["species_common_name"] for rule in declaration_rules)
        self.assertEqual(species, ["bar", "dorade rose", "lieu jaune"])
        self.assertTrue(all(rule["rule_type"] == "LOCAL_RESTRICTION" for rule in declaration_rules))

    def test_extract_practice_restriction_rules_detects_general_practice_rule(self) -> None:
        text = "La vente des produits issus de la peche maritime de loisir est interdite."

        rules = extract_practice_restriction_rules(self.dirm_source, "https://www.dirm.example/page.html", text)

        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0]["rule_type"], "PRACTICE_RULE")
        self.assertIn("vente", rules[0]["description"].lower())

    def test_extract_relevant_html_links_from_html(self) -> None:
        html = """
        <a href="/peche-de-loisir-reglementation-a100.html">Reglementation</a>
        <a href="/random-page-a101.html">Random</a>
        <a href="/docs/annexe-quotas.pdf">Annexe</a>
        """

        links = extract_relevant_html_links_from_html(
            html,
            "https://www.dirm.example/index.html",
            limit=5,
        )

        self.assertIn("https://www.dirm.example/peche-de-loisir-reglementation-a100.html", links)
        self.assertTrue(all(not link.endswith(".pdf") for link in links))

    def test_html_to_text_removes_scripts_and_preserves_blocks(self) -> None:
        html = """
        <nav>Coquilles Saint-Jacques</nav>
        <script>var mediabox_settings = {"debug": true};</script>
        <p>Les especes concernees sont le bar et le lieu jaune.</p>
        """

        text = html_to_text(html)

        self.assertNotIn("mediabox_settings", text)
        self.assertIn("Coquilles Saint-Jacques", text.splitlines()[0])
        self.assertIn("Les especes concernees", text.splitlines()[-1])

    def test_find_sentence_does_not_return_full_document(self) -> None:
        text = """
        Acceder au contenu Menu principal.
        Les pecheurs doivent declarer le bar via RecFishing.
        Mentions legales et pied de page.
        """

        sentence = find_sentence(text, ["bar", "declar"])

        self.assertEqual(sentence, "Les pecheurs doivent declarer le bar via RecFishing.")

    def test_protected_species_ignores_species_only_in_navigation(self) -> None:
        text = html_to_text(
            """
            <nav>Coquilles Saint-Jacques</nav>
            <main>
              <p>Cette page rappelle les tailles minimales et les especes interdites.</p>
            </main>
            """
        )

        rules = extract_dirm_protected_species_rules(self.dirm_source, "https://www.dirm.example/page.html", text)

        self.assertEqual(rules, [])

    def test_sensitive_declaration_uses_relevant_context_only(self) -> None:
        source = SourceRecord(
            source_id="dirm_memn_especes_sensibles_2026",
            source_type="DIRM",
            source_priority=2,
            authority_name="DIRM MEMN",
            title="Enregistrement des pecheurs de loisir et declaration des captures",
            url="https://www.dirm.example/enregistrement.html",
            kind="html",
        )
        text = html_to_text(
            """
            <nav>Coquilles Saint-Jacques Tourteau</nav>
            <main>
              <p>Especes concernees pour 2026.</p>
              <p>Lieu jaune, Bar et Dorade rose sont des especes sensibles.</p>
              <p>Les pecheurs doivent s'enregistrer et declarer les captures via RecFishing.</p>
            </main>
            <footer>Sur le meme sujet: Tourteau et coquillages.</footer>
            """
        )

        rules = extract_sensitive_species_declaration_rules(source, source.url, text)

        species = sorted(rule["species_common_name"] for rule in rules)
        self.assertEqual(species, ["bar", "dorade rose", "lieu jaune"])
        self.assertTrue(all(len(rule["description"]) < 500 for rule in rules))
        self.assertTrue(all("Coquilles" not in rule["description"] for rule in rules))

    def test_resolve_rule_zone_uses_source_scope(self) -> None:
        zone = resolve_rule_zone(self.dirm_source, "Taille minimale de capture du bar.")

        self.assertEqual(zone["zone_code"], "FACADE_NAMO")
        self.assertEqual(zone["strategy"], "CUSTOM_BBOX")

    def test_resolve_rule_zone_overrides_with_text_scope(self) -> None:
        zone = resolve_rule_zone(self.dirm_source, "Restriction locale dans le parc national des Calanques.")

        self.assertEqual(zone["zone_code"], "SECTEUR_CALANQUES")
        self.assertEqual(zone["strategy"], "CUSTOM_BBOX")

    def test_should_try_pdf_ocr_decision(self) -> None:
        self.assertTrue(should_try_pdf_ocr("texte court", enable_ocr=True, min_chars=100))
        self.assertFalse(should_try_pdf_ocr("texte court", enable_ocr=False, min_chars=100))
        self.assertFalse(should_try_pdf_ocr("x" * 200, enable_ocr=True, min_chars=100))

    def test_ai_local_endpoint_does_not_require_api_key_header(self) -> None:
        self.assertTrue(ai_base_url_is_local("http://localhost:11434/v1"))
        self.assertEqual(
            ai_request_headers("http://localhost:11434/v1", api_key=None),
            {"Content-Type": "application/json"},
        )

    def test_ai_remote_endpoint_uses_api_key_header(self) -> None:
        self.assertFalse(ai_base_url_is_local("https://openrouter.ai/api/v1"))
        self.assertEqual(
            ai_request_headers("https://openrouter.ai/api/v1", api_key="test-key")["Authorization"],
            "Bearer test-key",
        )

    def test_validate_rule_set_requires_core_categories(self) -> None:
        diving = build_base_rule(
            rule_key="r.diving",
            rule_type="DIVING_GENERAL",
            title="Diving",
            description="desc",
            source=self.legifrance_source,
            source_url=self.legifrance_source.url,
            legal_reference=None,
            metric_type=None,
            metric_value=None,
            metric_unit=None,
            species_common_name=None,
            species_scientific_name=None,
            needs_manual_review=False,
            notes="",
        )

        with self.assertRaises(RuntimeError):
            validate_rule_set([diving])

    def test_deduplicate_rules_merges_equivalent_content(self) -> None:
        first = build_base_rule(
            rule_key="species.bar.min-size.namo",
            rule_type="MIN_SIZE",
            title="Taille minimale bar",
            description="Bar commun : 42 cm",
            source=self.dirm_source,
            source_url="https://www.dirm.example/a.pdf",
            legal_reference=None,
            metric_type="SIZE_MIN_CM",
            metric_value=42,
            metric_unit="cm",
            species_common_name="bar",
            species_scientific_name=None,
            needs_manual_review=True,
            notes="",
            zone={"zone_code": "FACADE_NAMO", "zone_name": "NAMO", "strategy": "CUSTOM_BBOX", "lat_min": 46, "lat_max": 51, "lon_min": -6, "lon_max": 0},
        )
        second = dict(first)
        second["rule_key"] = "species.bar-europeen.min-size.namo"
        second["source"] = dict(first["source"], source_priority=1)

        rules = deduplicate_rules([first, second])

        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0]["rule_key"], "species.bar-europeen.min-size.namo")
        self.assertIn("duplicate_content_merged", rules[0]["quality_flags"])

    def test_quality_report_detects_metric_conflicts(self) -> None:
        base = build_base_rule(
            rule_key="species.bar.min-size.namo",
            rule_type="MIN_SIZE",
            title="Taille minimale bar",
            description="Bar commun : 42 cm",
            source=self.dirm_source,
            source_url="https://www.dirm.example/a.pdf",
            legal_reference=None,
            metric_type="SIZE_MIN_CM",
            metric_value=42,
            metric_unit="cm",
            species_common_name="bar",
            species_scientific_name=None,
            needs_manual_review=True,
            notes="",
            zone={"zone_code": "FACADE_NAMO", "zone_name": "NAMO", "strategy": "CUSTOM_BBOX", "lat_min": 46, "lat_max": 51, "lon_min": -6, "lon_max": 0},
        )
        conflict = dict(base)
        conflict["rule_key"] = "species.bar.min-size.namo.alt"
        conflict["metric_value"] = 45

        report = build_quality_report([base, conflict])

        categories = {issue["category"] for issue in report["issues"]}
        self.assertIn("metric_conflict", categories)

    def test_apply_ai_audit_marks_rule_for_manual_review(self) -> None:
        rule = build_base_rule(
            rule_key="species.bar.quota.namo",
            rule_type="QUOTA",
            title="Quota bar",
            description="Bar : 2 captures par jour",
            source=self.dirm_source,
            source_url="https://www.dirm.example/a.pdf",
            legal_reference=None,
            metric_type="QUOTA_MAX_UNITS",
            metric_value=2,
            metric_unit="captures/jour",
            species_common_name="bar",
            species_scientific_name=None,
            needs_manual_review=False,
            notes="",
        )

        apply_ai_audit_to_rules(
            [rule],
            {
                "status": "ok",
                "issues": [
                    {
                        "severity": "warning",
                        "category": "duplicate",
                        "rule_key": "species.bar.quota.namo",
                        "message": "Possible doublon.",
                    }
                ],
            },
        )

        self.assertTrue(rule["needs_manual_review"])
        self.assertIn("ai_duplicate", rule["quality_flags"])

    def test_normalize_ai_confidence_scores_accepts_list_and_percent(self) -> None:
        scores = normalize_ai_confidence_scores(
            {
                "confidence_scores": [
                    {
                        "rule_key": "r1",
                        "confidence_score": 87,
                        "confidence_reason": "Source officielle et valeur claire.",
                        "valid_from": "2026-05-01",
                        "effective_date_quote": "Le present arrete entre en vigueur le 1er mai 2026.",
                    }
                ]
            }
        )

        self.assertEqual(scores["r1"]["confidence_score"], 0.87)
        self.assertIn("Source officielle", scores["r1"]["confidence_reason"])
        self.assertEqual(scores["r1"]["valid_from"], "2026-05-01")

    def test_ai_message_content_to_text_accepts_openrouter_style_parts(self) -> None:
        text = ai_message_content_to_text(
            [
                {"type": "output_text", "text": "{\"issues\": [], \"confidence_scores\": []}"},
            ]
        )

        self.assertIn("\"issues\": []", text)

    def test_extract_first_json_object_accepts_wrapped_json(self) -> None:
        parsed = extract_first_json_object(
            "Voici le resultat:\n{\"issues\": [], \"confidence_scores\": {\"r1\": {\"confidence_score\": 0.8}}}\nMerci."
        )

        self.assertIn("issues", parsed)
        self.assertIn("confidence_scores", parsed)

    def test_extract_first_json_object_rejects_empty_response(self) -> None:
        with self.assertRaises(ValueError):
            extract_first_json_object("")

    def test_parse_french_date_to_iso_accepts_textual_and_numeric_dates(self) -> None:
        self.assertEqual(parse_french_date_to_iso("1er mai 2026"), "2026-05-01")
        self.assertEqual(parse_french_date_to_iso("30/04/2026"), "2026-04-30")
        self.assertEqual(parse_french_date_to_iso("31 mars", default_year=2026), "2026-03-31")

    def test_infer_rule_validity_from_explicit_effective_date(self) -> None:
        validity = infer_rule_validity("Le present arrete entre en vigueur le 1er mai 2026.")

        self.assertEqual(validity["valid_from"], "2026-05-01")
        self.assertEqual(validity["effective_date_source"], "parser")
        self.assertIn("entre en vigueur", validity["effective_date_quote"])

    def test_infer_rule_validity_from_source_title_year(self) -> None:
        validity = infer_rule_validity("", source_title="Bar et lieu jaune - Regles applicables en 2026")

        self.assertEqual(validity["valid_from"], "2026-01-01")
        self.assertEqual(validity["valid_to"], "2026-12-31")
        self.assertEqual(validity["effective_date_source"], "parser_title")

    def test_infer_rule_validity_does_not_use_unrelated_body_year_for_day_month_only(self) -> None:
        validity = infer_rule_validity(
            "Article L. 921-1 cree en 2007. La peche des crabes au moyen de casier est interdite du 15 mars au 15 avril."
        )

        self.assertEqual(validity, {})

    def test_infer_rule_validity_ignores_public_consultation_period(self) -> None:
        validity = infer_rule_validity(
            "Vu les observations formulees lors de la consultation du public realisee du 12 novembre au 2 decembre 2024 inclus. "
            "Aucun specimen de lieu jaune ne peut etre capture et detenu du 1er janvier au 30 avril.",
            source_url="https://www.example.gouv.fr/joe_20260411.pdf",
        )

        self.assertEqual(validity["valid_from"], "2026-01-01")
        self.assertEqual(validity["valid_to"], "2026-04-30")

    def test_source_context_window_keeps_anchor_with_more_context(self) -> None:
        context = source_context_window(
            "Intro. Bar commun (Dicentrarchus labrax) : 42 cm. Fin du tableau.",
            "Bar commun (Dicentrarchus labrax) : 42 cm",
            max_chars=80,
        )

        self.assertIn("Bar commun", context)
        self.assertIn("Intro.", context)

    def test_quote_matches_source_context_accepts_normalized_whitespace(self) -> None:
        self.assertTrue(
            quote_matches_source_context(
                "Bar commun : 42 cm",
                "Tableau 2026  Bar commun :   42 cm  facade NAMO",
            )
        )

    def test_parse_ai_response_payload_accepts_choices_message_content(self) -> None:
        parsed = parse_ai_response_payload(
            {
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": "{\"issues\": [], \"confidence_scores\": []}",
                        },
                    }
                ],
            }
        )

        self.assertEqual(parsed["issues"], [])

    def test_parse_ai_response_payload_accepts_openrouter_responses_output_shape(self) -> None:
        parsed = parse_ai_response_payload(
            {
                "id": "resp_123",
                "object": "response",
                "status": "completed",
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {"type": "output_text", "text": "{\"issues\": [], \"confidence_scores\": []}"}
                        ],
                    }
                ],
            }
        )

        self.assertEqual(parsed["confidence_scores"], [])

    def test_parse_ai_response_payload_surfaces_provider_error(self) -> None:
        with self.assertRaisesRegex(ValueError, "rate_limit_exceeded"):
            parse_ai_response_payload(
                {
                    "error": {
                        "code": "rate_limit_exceeded",
                        "message": "Rate limit exceeded",
                    }
                }
            )

    def test_summarize_ai_response_payload_includes_error_details(self) -> None:
        summary = summarize_ai_response_payload(
            {"object": "chat.completion", "error": {"code": 503, "message": "No provider available"}}
        )

        self.assertIn("object=chat.completion", summary)
        self.assertIn("error_code=503", summary)

    @patch("pscripts.regulations.build_regulations_feed.AI_API_KEY", "test-key")
    @patch("pscripts.regulations.build_regulations_feed.AI_MODEL", "nvidia/nemotron-3-super-120b-a12b:free")
    @patch("pscripts.regulations.build_regulations_feed.AI_BASE_URL", "https://openrouter.ai/api/v1")
    @patch("pscripts.regulations.build_regulations_feed.ENABLE_AI_AUDIT", True)
    @patch("pscripts.regulations.build_regulations_feed.requests.post")
    def test_run_ai_rule_audit_retries_openrouter_without_response_format(self, mock_post) -> None:
        class MockResponse:
            def __init__(self, status_code: int, payload: dict[str, object]) -> None:
                self.status_code = status_code
                self._payload = payload
                self.text = ""

            def json(self) -> dict[str, object]:
                return self._payload

        mock_post.side_effect = [
            MockResponse(200, {"status": "completed"}),
            MockResponse(
                200,
                {
                    "choices": [
                        {
                            "index": 0,
                            "finish_reason": "stop",
                            "message": {
                                "role": "assistant",
                                "content": "{\"issues\": [], \"confidence_scores\": []}",
                            },
                        }
                    ]
                },
            ),
        ]

        rule = build_base_rule(
            rule_key="species.bar.quota.namo",
            rule_type="QUOTA",
            title="Quota bar",
            description="Bar : 2 captures par jour",
            source=self.dirm_source,
            source_url="https://www.dirm.example/a.pdf",
            legal_reference=None,
            metric_type="QUOTA_MAX_UNITS",
            metric_value=2,
            metric_unit="captures/jour",
            species_common_name="bar",
            species_scientific_name=None,
            needs_manual_review=False,
            notes="",
        )

        audit = run_ai_rule_audit([rule])

        self.assertEqual(audit["status"], "ok")
        self.assertEqual(audit["attempt_count"], 2)
        self.assertEqual(mock_post.call_count, 2)
        first_payload = mock_post.call_args_list[0].kwargs["json"]
        second_payload = mock_post.call_args_list[1].kwargs["json"]
        self.assertIn("response_format", first_payload)
        self.assertNotIn("response_format", second_payload)

    def test_apply_ai_audit_sets_confidence_score(self) -> None:
        rule = build_base_rule(
            rule_key="species.bar.quota.namo",
            rule_type="QUOTA",
            title="Quota bar",
            description="Bar : 2 captures par jour",
            source=self.dirm_source,
            source_url="https://www.dirm.example/a.pdf",
            legal_reference=None,
            metric_type="QUOTA_MAX_UNITS",
            metric_value=2,
            metric_unit="captures/jour",
            species_common_name="bar",
            species_scientific_name=None,
            needs_manual_review=False,
            notes="",
        )

        apply_ai_audit_to_rules(
            [rule],
            {
                "status": "ok",
                "issues": [],
                "confidence_scores": {
                    "species.bar.quota.namo": {
                        "confidence_score": 0.58,
                        "confidence_reason": "Contexte trop court.",
                    }
                },
            },
        )

        self.assertEqual(rule["confidence_score"], 0.58)
        self.assertEqual(rule["confidence_source"], "ai")
        self.assertEqual(rule["confidence_reason"], "Contexte trop court.")
        self.assertTrue(rule["needs_manual_review"])
        self.assertIn("ai_low_confidence", rule["quality_flags"])

    def test_apply_ai_audit_keeps_selected_quote_only_if_from_source_context(self) -> None:
        rule = build_base_rule(
            rule_key="species.bar.quota.namo",
            rule_type="QUOTA",
            title="Quota bar",
            description="Bar : 2 captures par jour",
            source=self.dirm_source,
            source_url="https://www.dirm.example/a.pdf",
            legal_reference=None,
            metric_type="QUOTA_MAX_UNITS",
            metric_value=2,
            metric_unit="captures/jour",
            species_common_name="bar",
            species_scientific_name=None,
            needs_manual_review=False,
            notes="",
            source_excerpt="Bar : 2 captures par jour",
            source_context="Reglementation peche de loisir. Bar : 2 captures par jour. Zone NAMO.",
        )

        apply_ai_audit_to_rules(
            [rule],
            {
                "status": "ok",
                "issues": [],
                "confidence_scores": {
                    "species.bar.quota.namo": {
                        "confidence_score": 0.83,
                        "confidence_reason": "Extrait clair.",
                        "selected_quote": "Bar : 2 captures par jour",
                        "selected_quote_reason": "Phrase la plus normative.",
                    }
                },
            },
        )

        self.assertEqual(rule["selected_quote"], "Bar : 2 captures par jour")
        self.assertEqual(rule["selected_quote_source"], "ai")
        self.assertEqual(rule["selected_quote_reason"], "Phrase la plus normative.")

    def test_apply_ai_audit_accepts_effective_date_only_with_source_quote(self) -> None:
        rule = build_base_rule(
            rule_key="species.bar.quota.namo",
            rule_type="QUOTA",
            title="Quota bar",
            description="Bar : 2 captures par jour",
            source=self.dirm_source,
            source_url="https://www.dirm.example/a.pdf",
            legal_reference=None,
            metric_type="QUOTA_MAX_UNITS",
            metric_value=2,
            metric_unit="captures/jour",
            species_common_name="bar",
            species_scientific_name=None,
            needs_manual_review=False,
            notes="",
            source_context="Le present arrete entre en vigueur le 1er mai 2026. Bar : 2 captures par jour.",
        )
        rule["valid_from"] = None
        rule["effective_date_source"] = None

        apply_ai_audit_to_rules(
            [rule],
            {
                "status": "ok",
                "issues": [],
                "confidence_scores": {
                    "species.bar.quota.namo": {
                        "confidence_score": 0.82,
                        "confidence_reason": "Source claire.",
                        "valid_from": "2026-05-01",
                        "effective_date_quote": "Le present arrete entre en vigueur le 1er mai 2026.",
                        "effective_date_reason": "Date d'effet explicite.",
                    }
                },
            },
        )

        self.assertEqual(rule["valid_from"], "2026-05-01")
        self.assertEqual(rule["effective_date_source"], "ai")
        self.assertIn("entre en vigueur", rule["effective_date_quote"])

    def test_enrich_rule_for_publication_adds_audit_fields(self) -> None:
        rule = build_base_rule(
            rule_key="species.bar.min-size.namo",
            rule_type="MIN_SIZE",
            title="Taille minimale bar",
            description="Bar commun : 42 cm",
            source=self.dirm_source,
            source_url="https://www.dirm.example/a.pdf",
            legal_reference="Arrete test",
            metric_type="SIZE_MIN_CM",
            metric_value=42,
            metric_unit="cm",
            species_common_name="bar",
            species_scientific_name=None,
            needs_manual_review=True,
            notes="",
        )

        enriched = enrich_rule_for_publication(rule)

        self.assertEqual(enriched["status"], "needs_review")
        self.assertEqual(enriched["activity_type"], "recreational_fishing")
        self.assertEqual(enriched["constraint_type"], "min_size")
        self.assertGreater(enriched["confidence_score"], 0)
        self.assertEqual(enriched["confidence_source"], "heuristic")
        self.assertEqual(enriched["species"][0]["scientific_name"], "Dicentrarchus labrax")
        self.assertEqual(enriched["citations"][0]["source_url"], "https://www.dirm.example/a.pdf")
        self.assertEqual(enriched["citations"][0]["source_excerpt"], "Bar commun : 42 cm")
        self.assertIn("Bar commun : 42 cm", enriched["citations"][0]["source_context"])
        self.assertEqual(enriched["candidate"]["rule_key"], "species.bar.min-size.namo")

    def test_build_v2_artifacts_from_enriched_rules(self) -> None:
        rule = enrich_rule_for_publication(
            build_base_rule(
                rule_key="species.bar.quota.namo",
                rule_type="QUOTA",
                title="Quota bar",
                description="Bar : 2 captures par jour",
                source=self.dirm_source,
                source_url="https://www.dirm.example/a.pdf",
                legal_reference=None,
                metric_type="QUOTA_MAX_UNITS",
                metric_value=2,
                metric_unit="captures/jour",
                species_common_name="bar",
                species_scientific_name=None,
                needs_manual_review=True,
                notes="",
                source_excerpt="Bar : 2 captures par jour",
                source_context="Reglementation peche de loisir en mer. Bar : 2 captures par jour. Zone NAMO.",
            )
        )

        documents = build_source_documents_manifest([rule])
        candidates = build_rule_candidates([rule])

        self.assertEqual(len(documents), 1)
        self.assertEqual(documents[0]["document_hash"], rule["citations"][0]["document_hash"])
        self.assertEqual(len(documents[0]["chunks"]), 2)
        self.assertEqual(candidates[0]["document_hash"], documents[0]["document_hash"])
        self.assertIn("source_context", candidates[0])


if __name__ == "__main__":
    unittest.main()
