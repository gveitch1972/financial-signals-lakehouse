import os
import sys
from urllib.error import HTTPError

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
    assert "SP.POP.TOTL;NY.GDP.MKTP.CD;FP.CPI.TOTL.ZG" in url
    assert "format=json" in url
    assert "page=2" in url
    assert "per_page=500" in url
    assert "source=2" in url


def test_build_world_bank_url_omits_source_when_requested(monkeypatch):
    monkeypatch.delenv("MACRO_API_URL", raising=False)
    monkeypatch.delenv("MACRO_INDICATOR_CODES", raising=False)

    url = ingest_macro_data.build_world_bank_url(
        page=1,
        per_page=1000,
        indicator_codes=["SP.POP.TOTL"],
        include_source=False,
    )

    assert url.startswith("https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?")
    assert "source=" not in url


def test_fetch_indicator_records_retries_without_source_on_http_400(monkeypatch):
    seen_urls = []
    payload = [
        {"page": 1, "pages": 1, "per_page": 1000, "total": 1},
        [
            {
                "country": {"id": "US", "value": "United States"},
                "countryiso3code": "USA",
                "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                "date": "2024",
                "value": 341814420,
            }
        ],
    ]

    def fake_fetch_json(url):
        seen_urls.append(url)
        if len(seen_urls) == 1:
            raise HTTPError(url, 400, "Bad Request", hdrs=None, fp=None)
        return payload

    monkeypatch.setattr(ingest_macro_data, "fetch_json", fake_fetch_json)

    records = ingest_macro_data.fetch_indicator_records("SP.POP.TOTL", date_range="2024:2024")

    assert len(records) == 1
    assert "source=2" in seen_urls[0]
    assert "source=" not in seen_urls[1]


def test_fetch_indicator_records_retries_without_source_on_empty_results(monkeypatch):
    seen_urls = []
    empty_payload = [{"page": 1, "pages": 1, "per_page": 1000, "total": 0}, []]
    data_payload = [
        {"page": 1, "pages": 1, "per_page": 1000, "total": 1},
        [
            {
                "country": {"id": "US", "value": "United States"},
                "countryiso3code": "USA",
                "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                "date": "2024",
                "value": 341814420,
            }
        ],
    ]

    def fake_fetch_json(url):
        seen_urls.append(url)
        if "source=2" in url:
            return empty_payload
        return data_payload

    monkeypatch.setattr(ingest_macro_data, "fetch_json", fake_fetch_json)

    records = ingest_macro_data.fetch_indicator_records("SP.POP.TOTL", date_range="2024:2024")

    assert len(records) == 1
    assert "source=2" in seen_urls[0]
    assert "source=" not in seen_urls[1]


def test_fetch_json_retries_on_timeout(monkeypatch):
    url = "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json"
    payload = b'[{"page":1,"pages":1},[]]'
    calls = {"count": 0}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return payload

    def fake_urlopen(_request, timeout):
        calls["count"] += 1
        if calls["count"] == 1:
            raise TimeoutError("timed out")
        assert timeout == ingest_macro_data.DEFAULT_HTTP_TIMEOUT_SECONDS
        return FakeResponse()

    monkeypatch.setattr(ingest_macro_data, "urlopen", fake_urlopen)
    monkeypatch.setattr(ingest_macro_data, "DEFAULT_HTTP_MAX_RETRIES", 2)
    monkeypatch.setattr(ingest_macro_data, "DEFAULT_HTTP_RETRY_BACKOFF_SECONDS", 0.0)

    result = ingest_macro_data.fetch_json(url)

    assert calls["count"] == 2
    assert result == [{"page": 1, "pages": 1}, []]
