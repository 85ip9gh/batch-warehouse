"""Conforming rules, as pure functions over strings.

None of this imports Spark. The conforming decisions are where the judgement
lives and where the bugs hide, so they are ordinary functions that a test can
call directly, and the Spark layer is a thin thing that applies them. A rule
that can only be exercised by starting a session is a rule nobody exercises.

Every rule here was measured against the real corpus before it was written, and
the measurement is quoted where it justifies the rule.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Legal-form suffixes only. Measured on 6,006 distinct company strings:
# case-and-punctuation folding alone merges 54, adding these suffixes merges
# 127, and every one of those 127 is unambiguously the same employer
# ("Cummins" and "Cummins Inc.", "Mozilla" and "Mozilla Corporation").
#
# Country suffixes are deliberately NOT in this list. Adding `canada` and `usa`
# would merge only 19 more, and they are the wrong 19: "KPMG" and "KPMG Canada"
# are a global brand and a national entity, not two spellings of one name. For
# a warehouse about a national job market the national entity is the meaningful
# one, so merging them would destroy the distinction this data exists to show.
LEGAL_SUFFIX = re.compile(
    r"\b(?:inc|incorporated|ltd|limited|llc|llp|corp|corporation|plc|gmbh|pvt|pte)\b\.?",
    re.IGNORECASE,
)

CANADIAN_PROVINCES = {
    "AB": "Alberta",
    "BC": "British Columbia",
    "MB": "Manitoba",
    "NB": "New Brunswick",
    "NL": "Newfoundland and Labrador",
    "NS": "Nova Scotia",
    "NT": "Northwest Territories",
    "NU": "Nunavut",
    "ON": "Ontario",
    "PE": "Prince Edward Island",
    "QC": "Quebec",
    "SK": "Saskatchewan",
    "YT": "Yukon",
}

US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}

COUNTRY_TOKENS = {
    "CA": "CA", "CAN": "CA", "CANADA": "CA",
    "US": "US", "USA": "US", "UNITED STATES": "US",
    "UNITED STATES OF AMERICA": "US",
}

COUNTRY_NAMES = {"CA": "Canada", "US": "United States"}

_PROVINCE_BY_NAME = {name.upper(): code for code, name in CANADIAN_PROVINCES.items()}
_STATE_BY_NAME = {name.upper(): code for code, name in US_STATES.items()}

# A Canadian postal code, which turns up in the last comma-separated slot for
# some employer ATS feeds ("Toronto-Data Engineer-ON-M5H 1H1").
_POSTAL_CODE = re.compile(r"^[A-Z]\d[A-Z]\s*\d[A-Z]\d$", re.IGNORECASE)


@dataclass(frozen=True)
class Location:
    """A conformed location, or an honest admission that it is not conformed."""

    city: str | None
    province_code: str | None
    province: str | None
    country_code: str | None
    country: str | None
    resolved: bool
    raw: str

    @property
    def key(self) -> str:
        """Stable identity for `dim_location`.

        Unresolved locations key on their raw string. They get a dimension row
        of their own rather than being folded into a single "unknown" bucket or
        dropped, because a fact pointing at a row that says "this was never
        parsed, here is exactly what arrived" is auditable and a fact pointing
        at nothing is not.
        """
        if not self.resolved:
            return f"raw:{self.raw.strip().casefold()}"
        return "|".join(
            part or "" for part in (self.city or "", self.province_code, self.country_code)
        ).casefold()


def conform_company(raw: str | None) -> tuple[str | None, str | None]:
    """Return the conforming key and the display name for a company string.

    The key folds case, punctuation and legal-form suffix. The display name is
    the raw string, untouched, because the dimension should show the employer
    the way the posting wrote it and only *group* by the key.
    """
    if raw is None:
        return None, None
    display = raw.strip()
    if not display:
        return None, None
    key = re.sub(r"[^a-z0-9]", "", LEGAL_SUFFIX.sub("", display).lower())
    if not key:
        # A name that is nothing but a legal suffix, or nothing but punctuation.
        # Falling back to the folded raw string keeps it distinguishable instead
        # of collapsing every such name onto one empty key.
        key = re.sub(r"[^a-z0-9]", "", display.lower())
    return (key or None), display


def _classify(token: str) -> tuple[str, str] | None:
    """Identify a single location token as a province or a state.

    Returns (country_code, province_code) or None. Note this never resolves the
    bare token "CA": that decision belongs to the caller, which knows the
    token's position, and position is the only thing that disambiguates it.
    """
    upper = token.strip().upper()
    if not upper:
        return None
    if upper in CANADIAN_PROVINCES:
        return "CA", upper
    if upper in _PROVINCE_BY_NAME:
        return "CA", _PROVINCE_BY_NAME[upper]
    if upper in US_STATES:
        return "US", upper
    if upper in _STATE_BY_NAME:
        return "US", _STATE_BY_NAME[upper]
    return None


def parse_location(raw: str | None) -> Location:
    """Parse a location string into conformed parts, or mark it unresolved.

    Works from the right, because that is the only end with a reliable meaning:
    the leading parts can be a city, a building, a street, or several cities.

    **"CA" is the trap and position is the only thing that resolves it.** In the
    corpus it appears 7,331 times as the last token, meaning Canada, and 881
    times as the second-to-last token, meaning California. A lookup that treats
    the token alone would file every San Francisco posting in Canada or every
    Toronto posting in California, and either way the country breakdown that
    this warehouse exists to produce would be quietly wrong.
    """
    text = (raw or "").strip()
    if not text:
        return Location(None, None, None, None, None, False, text)

    parts = [p.strip() for p in text.split(",") if p.strip()]
    if not parts:
        return Location(None, None, None, None, None, False, text)

    country_code: str | None = None
    # Only the LAST token may be read as a country. This is what keeps "CA" in
    # the province slot as California.
    if COUNTRY_TOKENS.get(parts[-1].upper()):
        country_code = COUNTRY_TOKENS[parts[-1].upper()]
        parts = parts[:-1]
    elif _POSTAL_CODE.match(parts[-1]):
        # A postal code in the country slot is noise, not a country. Dropping it
        # lets the province and city behind it still resolve.
        parts = parts[:-1]

    province_code: str | None = None
    if parts:
        classified = _classify(parts[-1])
        if classified is not None:
            inferred_country, province_code = classified
            if country_code is None:
                country_code = inferred_country
            elif country_code != inferred_country:
                # "Toronto, TX, CA" and its kind. The two halves disagree and
                # guessing which is right would invent a fact, so the whole
                # string is unresolved and keeps its raw form.
                return Location(None, None, None, None, None, False, text)
            parts = parts[:-1]

    city = ", ".join(parts) if parts else None

    # "Remote" is a working arrangement, not a place. Leaving it in the city
    # field would create a dim_location row for a city that does not exist and
    # scatter genuinely remote postings across one per country.
    if city and city.strip().lower() == "remote":
        city = None

    resolved = province_code is not None or country_code is not None
    if not resolved:
        return Location(None, None, None, None, None, False, text)

    province = None
    if province_code is not None:
        table = CANADIAN_PROVINCES if country_code == "CA" else US_STATES
        province = table.get(province_code)

    return Location(
        city=city,
        province_code=province_code,
        province=province,
        country_code=country_code,
        country=COUNTRY_NAMES.get(country_code or ""),
        resolved=True,
        raw=text,
    )


def parse_amount(raw: str | None) -> float | None:
    """A salary figure, or None. Never zero as a stand-in for missing.

    Zero is a real number and a posting that pays nothing is different from a
    posting that did not say. Collapsing the two would put the second into every
    average.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    if value != value or value in (float("inf"), float("-inf")):
        return None
    return value


# Measured across 24,636 landed observations: 22,478 are a plain ISO date,
# 2,146 (8.7%) are empty, and 12 carry a midnight timestamp suffix. Every one of
# the 12 is recoverable, so nothing is lost by handling the second shape.
_ISO_DATE = re.compile(r"^(\d{4}-\d{2}-\d{2})(?:[ T]\d{2}:\d{2}:\d{2}.*)?$")


def parse_posted_date(raw: str | None) -> str | None:
    """The posting date as an ISO string, or None when it was not stated.

    Empty and malformed both return None, and they are not the same thing: 8.7%
    of observations simply carry no date, which is a fact about the source
    rather than a defect. What matters is that neither becomes a real-looking
    date. Spark 4 runs ANSI casting by default and fails the job on a malformed
    value rather than nulling it quietly, which is how the empties were found at
    all, so the parsing happens here where it can be reasoned about.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    match = _ISO_DATE.match(text)
    if not match:
        return None
    candidate = match.group(1)
    year, month, day = (int(part) for part in candidate.split("-"))
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    return candidate
