import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import src.bronze.ingest_macro_data as ingest_macro_data


def test_parse_indicator_codes_from_env(monkeypatch):
    monkeypatch.setenv("MACRO_INDICATOR_CODES", "SP.POP.TOTL, NY.GDP.MKTP.CD ; FP.CPI.TOTL.ZG")

    assert ingest_macro_data.parse_indicator_codes() == [
        "SP.POP.TOTL",
        "NY.GDP.MKTP.CD",
        "FP.CPI.TOTL.ZG",
    ]


def test_normalize_world_bank_payload_flattens_rows():
    payload = [
        {"page": 1, "pages": 1, "per_page": 1000, "total": 2},
        [
            {
                "country": {"id": "US", "value": "United States"},
                "countryiso3code": "USA",
                "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                "date": "2024",
                "value": 341814420,
            },
            {
                "country": {"id": "GB", "value": "United Kingdom"},
                "countryiso3code": "GBR",
                "indicator": {"id": "FP.CPI.TOTL.ZG", "value": "Inflation, consumer prices (annual %)"},
                "date": "2024",
                "value": 3.2,
            },
        ],
    ]

    records, page_count = ingest_macro_data.normalize_world_bank_payload(payload)

    assert page_count == 1
    assert records == [
        {
            "country_code": "USA",
            "indicator_name": "Population, total",
            "observation_date": "2024",
            "observation_value": 341814420,
            "raw_payload": '{"country": {"id": "US", "value": "United States"}, "countryiso3code": "USA", "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"}, "date": "2024", "value": 341814420}',
        },
        {
            "country_code": "GBR",
            "indicator_name": "Inflation, consumer prices (annual %)",
            "observation_date": "2024",
            "observation_value": 3.2,
            "raw_payload": '{"country": {"id": "GB", "value": "United Kingdom"}, "countryiso3code": "GBR", "indicator": {"id": "FP.CPI.TOTL.ZG", "value": "Inflation, consumer prices (annual %)"}, "date": "2024", "value": 3.2}',
        },
    ]


def test_build_world_bank_url_uses_expected_defaults(monkeypatch):
    monkeypatch.delenv("MACRO_API_URL", raising=False)
    monkeypatch.delenv("MACRO_INDICATOR_CODES", raising=False)

    url = ingest_macro_data.build_world_bank_url(page=2, per_page=500)

    assert url.startswith("https://api.worldbank.org/v2/country/all/indicator/")
    assert "SP.POP.TOTL%3BNY.GDP.MKTP.CD%3BFP.CPI.TOTL.ZG" in url
    assert "format=json" in url
    assert "page=2" in url
    assert "per_page=500" in url
    assert "source=2" in url
