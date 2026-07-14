import ast
import datetime
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from settings.async_amo_api import AmoCustomers, AmoLead, AmoResult, AmoResultAnalizeCustomers
from utils.analytics import (
    analyze_and_send_to_sheets,
    analyze_customers_and_send_to_sheets,
    build_customers_analysis_payload,
    build_leads_payload,
)
from utils.files import cleanup_generated_file
from utils.formatting import format_grouped_number
from utils.tracking import (
    get_cookie_value,
    get_tracking_value,
    normalize_tracking_value,
    parse_sourcebuster_cookie,
)


def _lead(
        *,
        lead_id: int = 1,
        lead_price=100,
        contact_id: int = 10,
        paid_at=None,
        shipment_at=1,
) -> AmoLead:
    return AmoLead(
        lead_id=lead_id,
        lead_price=lead_price,
        created_at=0,
        close_at=0,
        contact_id=contact_id,
        shipment_at=shipment_at,
        paid_at=paid_at,
    )


def _customer(*, customer_id: int = 20, contact_id: int = 10) -> AmoCustomers:
    return AmoCustomers(
        customer_id=customer_id,
        created_at=0,
        contacts_id=[contact_id],
        status="active",
    )


class FormattingTests(unittest.TestCase):
    def test_formats_grouped_numbers(self):
        cases = {
            None: "",
            "": "",
            1234567: "1 234 567",
            "1234567,50": "1 234 567,5",
            -1234.5: "-1 234,5",
            "not-a-number": "not-a-number",
        }

        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(format_grouped_number(value), expected)


class TrackingTests(unittest.TestCase):
    def test_normalizes_empty_and_sentinel_values(self):
        for value in (None, "", "  ", "none", "(None)", "NULL", "undefined"):
            with self.subTest(value=value):
                self.assertIsNone(normalize_tracking_value(value))
        self.assertEqual(normalize_tracking_value(" campaign "), "campaign")

    def test_reads_cookie_with_whitespace_in_key(self):
        self.assertEqual(get_cookie_value({" utm_source ": "cookie"}, "utm_source"), "cookie")
        self.assertIsNone(get_cookie_value({}, "utm_source"))

    def test_parses_urlencoded_sourcebuster_cookie(self):
        value = "src%3Dyandex%7C%7C%7Cmdm%3Dcpc%7C%7C%7Cignored%7C%7C%7Ccmp%3Done%3Dtwo"
        self.assertEqual(
            parse_sourcebuster_cookie(value),
            {"src": "yandex", "mdm": "cpc", "cmp": "one=two"},
        )

    def test_tracking_value_uses_expected_source_priority(self):
        base = {
            "key": "utm_source",
            "sbjs_current": {"src": "current"},
            "sbjs_first": {"src": "first"},
            "sbjs_key": "src",
        }

        self.assertEqual(
            get_tracking_value(query_params={"utm_source": "query"}, cookies={"utm_source": "cookie"}, **base),
            "query",
        )
        self.assertEqual(
            get_tracking_value(query_params={"utm_source": "none"}, cookies={"utm_source": "cookie"}, **base),
            "cookie",
        )
        self.assertEqual(
            get_tracking_value(query_params={}, cookies={}, **base),
            "current",
        )
        self.assertEqual(
            get_tracking_value(
                query_params={},
                cookies={},
                **{**base, "sbjs_current": {"src": "null"}},
            ),
            "first",
        )

    def test_tracking_value_supports_alternative_cookie_keys(self):
        self.assertEqual(
            get_tracking_value(
                query_params={},
                cookies={"_ym_uid": "123"},
                key="yclid",
                sbjs_current={},
                sbjs_first={},
                sbjs_key="id",
                cookie_keys=("yclid", "_ym_uid"),
            ),
            "123",
        )


class AnalyticsPayloadTests(unittest.TestCase):
    def test_builds_leads_payload(self):
        lead = _lead(lead_id=7, lead_price=150.5, contact_id=11, paid_at=0)
        lead.clean_price = 25
        lead.last_buy = 172800
        lead.time_from_attestate = 86400
        result = AmoResult(lead_obj=lead, customer_obj=_customer(customer_id=30, contact_id=11))
        expected_shipment_at = datetime.datetime.fromtimestamp(1).strftime("%d-%m-%Y %H:%M:%S")

        self.assertEqual(
            build_leads_payload([result]),
            [{
                "lead_id": 7,
                "lead_price": 150.5,
                "created_at": "",
                "close_at": "",
                "shipment_at": expected_shipment_at,
                "attestate_at": "",
                "contact_id": 11,
                "customer_id": 30,
                "clean_price": 25,
                "last_buy": "2",
                "time_from_attestate": "1",
                "paid_at": "",
            }],
        )

    def test_customer_payload_handles_boundaries_and_invalid_amounts(self):
        prices_and_dates = [
            ("100,5", datetime.datetime(2023, 7, 1, 0, 0, 0)),
            (200, datetime.datetime(2024, 7, 1, 0, 0, 0)),
            (300, datetime.datetime(2025, 7, 1, 0, 0, 0)),
            (400, datetime.datetime(2026, 1, 1, 0, 0, 0)),
            (500, datetime.datetime(2026, 4, 1, 0, 0, 0)),
            (600, datetime.datetime(2026, 6, 30, 23, 59, 59)),
            (700, datetime.datetime(2026, 7, 1, 0, 0, 0)),
            ("invalid", datetime.datetime(2026, 5, 1, 0, 0, 0)),
            (50, None),
        ]
        leads = [
            _lead(lead_id=index, lead_price=price, paid_at=paid_at.timestamp() if paid_at else "invalid")
            for index, (price, paid_at) in enumerate(prices_and_dates, start=1)
        ]
        result = AmoResultAnalizeCustomers(lead_list=leads, customer_obj=_customer())

        self.assertEqual(
            build_customers_analysis_payload([result]),
            [{
                "customer_id": 20,
                "status": "active",
                "leads_count": 9,
                "clean_budjet": 2850.5,
                "clean_budjet_1": 2100.5,
                "clean_budjet_2": 2000,
                "clean_budjet_3": 1800,
                "clean_budjet_4": 1500,
                "clean_budjet_5": 1100,
            }],
        )


class _FakeSheets:
    def __init__(self):
        self.calls = []

    def send_json(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(status_code=200)


class AnalyticsBackgroundTaskTests(unittest.IsolatedAsyncioTestCase):
    async def test_lead_analysis_fetches_concurrently_and_sends_payload(self):
        lead = _lead()
        customer = _customer()
        api = SimpleNamespace(
            get_pipeline_1628622_status_142_leads=AsyncMock(return_value=[lead]),
            get_customers_with_contacts=AsyncMock(return_value=[customer]),
        )
        sheets = _FakeSheets()

        await analyze_and_send_to_sheets(
            amo_api=api,
            google_sheets=sheets,
            token="token",
            request_id="lead-request",
        )

        api.get_pipeline_1628622_status_142_leads.assert_awaited_once_with()
        api.get_customers_with_contacts.assert_awaited_once_with()
        self.assertEqual(len(sheets.calls), 1)
        self.assertEqual(sheets.calls[0]["request_id"], "lead-request")
        self.assertEqual(sheets.calls[0]["payload"][0]["lead_id"], 1)

    async def test_customer_analysis_sends_payload_to_customer_client(self):
        lead = _lead()
        customer = _customer()
        api = SimpleNamespace(
            get_pipeline_1628622_status_142_leads=AsyncMock(return_value=[lead]),
            get_customers_with_contacts=AsyncMock(return_value=[customer]),
        )
        main_sheets = _FakeSheets()
        customer_sheets = _FakeSheets()

        await analyze_customers_and_send_to_sheets(
            amo_api=api,
            google_sheets=main_sheets,
            google_sheets_customers=customer_sheets,
            token="token",
            request_id="customer-request",
        )

        api.get_pipeline_1628622_status_142_leads.assert_awaited_once_with()
        api.get_customers_with_contacts.assert_awaited_once_with()
        self.assertEqual(main_sheets.calls, [])
        self.assertEqual(len(customer_sheets.calls), 1)
        self.assertEqual(customer_sheets.calls[0]["request_id"], "customer-request")
        self.assertEqual(customer_sheets.calls[0]["payload"][0]["customer_id"], 20)


class FileCleanupTests(unittest.TestCase):
    def test_removes_existing_file_and_ignores_missing_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "generated.pdf"
            file_path.write_bytes(b"pdf")

            cleanup_generated_file(file_path)
            self.assertFalse(file_path.exists())

            cleanup_generated_file(file_path)


class MainModuleStructureTests(unittest.TestCase):
    def test_main_contains_only_decorated_functions(self):
        main_path = Path(__file__).resolve().parents[1] / "main.py"
        module = ast.parse(main_path.read_text(encoding="utf-8"))
        top_level_functions = [
            node
            for node in module.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]

        self.assertTrue(top_level_functions)
        self.assertTrue(all(node.decorator_list for node in top_level_functions))


if __name__ == "__main__":
    unittest.main()
