from __future__ import annotations

import unittest

from pscripts.regulations.discover_regulation_sources import (
    DiscoveryDomain,
    discover_from_seed,
    discover_from_sitemap,
    discover_links_from_html,
)


class DiscoverRegulationSourcesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.domain = DiscoveryDomain(
            host="www.dirm.example",
            source_type="DIRM",
            authority_name="DIRM Test",
            source_priority=2,
            seed_urls=("https://www.dirm.example/peche-de-loisir-r61.html",),
            sitemap_urls=(),
        )
        self.config = {
            "min_auto_accept_score": 10,
            "max_pages_per_seed": 5,
            "max_candidates_per_seed": 10,
            "current_year_window": [0, 1],
            "keywords": ["peche de loisir", "reglementation", "arrete", "thon rouge"],
            "negative_keywords": ["contact"],
        }

    def test_discover_links_from_html_accepts_relevant_official_links(self) -> None:
        html = """
        <a href="/nouvelle-reglementation-peche-de-loisir-a999.html">Nouvelle reglementation peche de loisir 2027</a>
        <a href="/contact">Contact</a>
        <a href="https://other.example/arrete-peche.pdf">Arrete externe</a>
        """

        candidates = discover_links_from_html(
            html=html,
            base_url="https://www.dirm.example/peche-de-loisir-r61.html",
            domain=self.domain,
            config=self.config,
            existing_urls=set(),
            discovery_method="test",
        )

        urls = {candidate["url"]: candidate for candidate in candidates}
        self.assertIn("https://www.dirm.example/nouvelle-reglementation-peche-de-loisir-a999.html", urls)
        self.assertEqual(
            urls["https://www.dirm.example/nouvelle-reglementation-peche-de-loisir-a999.html"]["status"],
            "auto_accepted",
        )
        self.assertNotIn("https://other.example/arrete-peche.pdf", urls)

    def test_discover_from_seed_crawls_auto_accepted_children(self) -> None:
        pages = {
            "https://www.dirm.example/peche-de-loisir-r61.html": (
                '<a href="/reglementation-peche-de-loisir-2027-a1.html">Reglementation peche de loisir 2027</a>'
            ),
            "https://www.dirm.example/reglementation-peche-de-loisir-2027-a1.html": (
                '<a href="/IMG/pdf/arrete-peche-de-loisir-2027.pdf">Arrete peche de loisir 2027</a>'
            ),
        }

        candidates = discover_from_seed(
            seed_url="https://www.dirm.example/peche-de-loisir-r61.html",
            domain=self.domain,
            config=self.config,
            existing_urls=set(),
            fetcher=lambda url: pages[url],
        )

        urls = {candidate["url"] for candidate in candidates}
        self.assertIn("https://www.dirm.example/reglementation-peche-de-loisir-2027-a1.html", urls)
        self.assertIn("https://www.dirm.example/IMG/pdf/arrete-peche-de-loisir-2027.pdf", urls)

    def test_discover_from_sitemap_scores_future_regulation_urls(self) -> None:
        sitemap = """
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>https://www.dirm.example/reglementation-peche-de-loisir-2027-a1.html</loc></url>
          <url><loc>https://www.dirm.example/contact</loc></url>
        </urlset>
        """

        candidates = discover_from_sitemap(
            sitemap_url="https://www.dirm.example/sitemap.xml",
            domain=self.domain,
            config=self.config,
            existing_urls=set(),
            fetcher=lambda _url: sitemap,
        )

        urls = {candidate["url"]: candidate for candidate in candidates}
        self.assertEqual(
            urls["https://www.dirm.example/reglementation-peche-de-loisir-2027-a1.html"]["status"],
            "auto_accepted",
        )


if __name__ == "__main__":
    unittest.main()
