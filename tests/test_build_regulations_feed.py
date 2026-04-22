from __future__ import annotations

import unittest

from pscripts.regulations.build_regulations_feed import (
    SourceRecord,
    build_base_rule,
    extract_dirm_closure_rules,
    extract_dirm_protected_species_rules,
    extract_dirm_quota_rules,
    extract_dirm_size_rules,
    extract_pdf_url_from_html,
    extract_relevant_html_links_from_html,
    extract_sensitive_species_declaration_rules,
    parse_legifrance_diving_rules,
    parse_legifrance_spearfishing_rules,
    parse_ministere_spearfishing_rules,
    resolve_rule_zone,
    should_try_pdf_ocr,
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


if __name__ == "__main__":
    unittest.main()
