"""Conforming tests. No Spark, no JVM, no infrastructure of any kind.

These are the rules that decide what the warehouse says, so they are tested as
ordinary functions rather than through a session.
"""

from __future__ import annotations

import pytest

from warehouse.conform import conform_company, parse_amount, parse_location, parse_posted_date


class TestCompanyConforming:
    @pytest.mark.parametrize(
        "left,right",
        [
            ("Cummins", "Cummins Inc."),
            ("Mozilla", "Mozilla Corporation"),
            ("AltaGas", "AltaGas Ltd."),
            ("Availity", "Availity, LLC."),
            ("Shannex", "Shannex Incorporated"),
            ("GeoSpectrum Technologies", "GeoSpectrum Technologies, Inc"),
            ("HighArc", "Higharc"),
            ("PLATO", "Plato"),
            ("City of Waterloo", "CITY OF WATERLOO"),
        ],
    )
    def test_merges_spellings_of_one_employer(self, left: str, right: str) -> None:
        assert conform_company(left)[0] == conform_company(right)[0]

    @pytest.mark.parametrize(
        "left,right",
        [
            # A global brand and a national entity are different employers, and
            # for a warehouse about a national job market the distinction is the
            # point. Merging these was measured and rejected.
            ("KPMG", "KPMG Canada"),
            ("Best Buy", "Best Buy Canada"),
            ("Esri", "Esri Canada"),
            ("The Home Depot", "The Home Depot Canada"),
            # Genuinely different companies that share a prefix.
            ("Scout Motors", "Scout Security"),
        ],
    )
    def test_keeps_distinct_employers_apart(self, left: str, right: str) -> None:
        assert conform_company(left)[0] != conform_company(right)[0]

    def test_keeps_the_raw_spelling_as_the_display_name(self) -> None:
        key, display = conform_company("  Remarcable, Inc.  ")
        assert display == "Remarcable, Inc."
        assert key == "remarcable"

    @pytest.mark.parametrize("raw", [None, "", "   "])
    def test_treats_a_missing_company_as_missing(self, raw: str | None) -> None:
        # 445 observations carry a blank company. They must not all collapse
        # onto one key that reads like a real employer.
        assert conform_company(raw) == (None, None)

    def test_a_name_that_is_only_a_legal_suffix_keeps_an_identity(self) -> None:
        # Stripping would leave an empty key and fold every such name together.
        assert conform_company("Limited")[0] == "limited"


class TestLocationParsing:
    def test_the_two_spellings_of_one_place_conform(self) -> None:
        abbreviated = parse_location("Toronto, ON, CA")
        spelled_out = parse_location("Toronto, Ontario, Canada")
        assert abbreviated.key == spelled_out.key
        assert abbreviated.province == spelled_out.province == "Ontario"
        assert abbreviated.country == spelled_out.country == "Canada"

    def test_ca_is_canada_in_the_country_slot(self) -> None:
        # 7,331 observations. Getting this wrong files every Canadian posting
        # under California.
        parsed = parse_location("Halifax, NS, CA")
        assert parsed.country_code == "CA"
        assert parsed.province == "Nova Scotia"
        assert parsed.city == "Halifax"

    def test_ca_is_california_in_the_province_slot(self) -> None:
        # 881 observations. The same two letters, and only position separates
        # them.
        parsed = parse_location("San Francisco, CA, US")
        assert parsed.country_code == "US"
        assert parsed.province == "California"
        assert parsed.city == "San Francisco"

    def test_a_bare_ca_alone_reads_as_the_country(self) -> None:
        parsed = parse_location("CA")
        assert parsed.resolved is True
        assert parsed.country_code == "CA"
        assert parsed.province_code is None

    @pytest.mark.parametrize(
        "raw,city,province,country",
        [
            ("Vancouver, British Columbia, Canada", "Vancouver", "British Columbia", "Canada"),
            ("Calgary, AB, CA", "Calgary", "Alberta", "Canada"),
            ("Austin, TX, US", "Austin", "Texas", "United States"),
            ("Boston, Massachusetts, United States", "Boston", "Massachusetts", "United States"),
            ("Ottawa, Ontario", "Ottawa", "Ontario", "Canada"),
            ("Seattle, WA", "Seattle", "Washington", "United States"),
        ],
    )
    def test_parses_every_shape_the_corpus_actually_contains(
        self, raw: str, city: str, province: str, country: str
    ) -> None:
        parsed = parse_location(raw)
        assert (parsed.city, parsed.province, parsed.country) == (city, province, country)

    def test_infers_the_country_from_the_province_when_none_is_given(self) -> None:
        assert parse_location("Halifax, NS").country_code == "CA"
        assert parse_location("Dallas, TX").country_code == "US"

    def test_drops_a_postal_code_masquerading_as_a_country(self) -> None:
        # Some employer ATS feeds put one in the last slot. Left alone it blocks
        # the province and city behind it from resolving.
        parsed = parse_location("Toronto, ON, M5H 1H1")
        assert parsed.city == "Toronto"
        assert parsed.province == "Ontario"
        assert parsed.country_code == "CA"

    def test_remote_is_not_a_city(self) -> None:
        # "Remote" is a working arrangement. As a city it would invent a
        # dim_location row and split remote postings one per country.
        parsed = parse_location("Remote, US")
        assert parsed.city is None
        assert parsed.country_code == "US"
        assert parsed.resolved is True

    def test_refuses_to_guess_when_the_province_and_country_disagree(self) -> None:
        # "Toronto, TX, CA" has a US state and a Canadian country. Picking one
        # would invent a fact.
        parsed = parse_location("Toronto, TX, CA")
        assert parsed.resolved is False
        assert parsed.raw == "Toronto, TX, CA"

    @pytest.mark.parametrize(
        "raw",
        [
            "16 YORK ST:TORONTO",
            "Toronto Headquarters",
            "Montreal - 1000 Rue; Calgary - 8th Ave SW; Halifax - Mumford Rd",
            "",
            None,
        ],
    )
    def test_marks_junk_unresolved_instead_of_inventing_a_place(self, raw: str | None) -> None:
        parsed = parse_location(raw)
        assert parsed.resolved is False
        assert parsed.city is None and parsed.country_code is None

    def test_unresolved_locations_keep_separate_identities(self) -> None:
        # They each get their own dimension row rather than sharing one
        # "unknown" bucket, so a fact always points at something that records
        # exactly what arrived.
        first = parse_location("16 YORK ST:TORONTO")
        second = parse_location("Toronto Headquarters")
        assert first.key != second.key
        assert first.key.startswith("raw:")

    def test_the_key_ignores_case_and_spelling_but_not_the_place(self) -> None:
        assert parse_location("halifax, ns, ca").key == parse_location("Halifax, Nova Scotia, Canada").key
        assert parse_location("Halifax, NS, CA").key != parse_location("Dartmouth, NS, CA").key


class TestAmounts:
    @pytest.mark.parametrize("raw,expected", [("120000", 120000.0), ("85000.50", 85000.5), ("0", 0.0)])
    def test_reads_a_figure(self, raw: str, expected: float) -> None:
        assert parse_amount(raw) == expected

    @pytest.mark.parametrize("raw", [None, "", "   ", "competitive", "nan-ish"])
    def test_missing_stays_missing_rather_than_becoming_zero(self, raw: str | None) -> None:
        # Zero is a real salary. A posting that did not say is not one that pays
        # nothing, and collapsing them would drag every average down.
        assert parse_amount(raw) is None

    @pytest.mark.parametrize("raw", ["nan", "inf", "-inf"])
    def test_rejects_the_float_values_that_poison_an_average(self, raw: str) -> None:
        assert parse_amount(raw) is None


class TestPostedDate:
    def test_reads_a_plain_iso_date(self) -> None:
        assert parse_posted_date("2026-08-03") == "2026-08-03"

    def test_recovers_the_twelve_rows_carrying_a_midnight_timestamp(self) -> None:
        # All 12 malformed values in the corpus are this shape, and all 12 are
        # recoverable, so handling it loses nothing and gains real dates.
        assert parse_posted_date("2026-08-03 00:00:00") == "2026-08-03"
        assert parse_posted_date("2026-08-04T09:15:00Z") == "2026-08-04"

    @pytest.mark.parametrize("raw", [None, "", "   "])
    def test_a_missing_date_is_none_rather_than_an_invented_one(self, raw: str | None) -> None:
        # 8.7% of observations state no date. That is a fact about the source,
        # and defaulting it to the ingest date would fabricate a posting date
        # for one row in twelve.
        assert parse_posted_date(raw) is None

    @pytest.mark.parametrize("raw", ["yesterday", "2026-13-01", "2026-08-00", "03/08/2026", "2026-8-3"])
    def test_refuses_anything_it_cannot_read_exactly(self, raw: str) -> None:
        assert parse_posted_date(raw) is None
