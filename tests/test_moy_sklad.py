import base64
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import dotenv
import httpx

from settings.moy_sklad import (
    MoySkladAPIError,
    MoySkladClient,
    _run_cli,
    save_token_to_env,
)


class MoySkladQueryTests(unittest.TestCase):
    def test_builds_moysklad_query_parameters(self):
        params = MoySkladClient.build_query_params(
            params={"search": "chair"},
            filters=["updated>=2026-01-01 00:00", "applicable=true"],
            expand=["positions", "state"],
            order=["updated,desc", "name,asc"],
            limit=100,
            offset=20,
        )

        self.assertEqual(params["search"], "chair")
        self.assertEqual(
            params["filter"],
            "updated>=2026-01-01 00:00;applicable=true",
        )
        self.assertEqual(params["expand"], "positions,state")
        self.assertEqual(params["order"], "updated,desc;name,asc")
        self.assertEqual(params["limit"], 100)
        self.assertEqual(params["offset"], 20)

    def test_validates_pagination_parameters(self):
        with self.assertRaisesRegex(ValueError, "between 1 and 1000"):
            MoySkladClient.build_query_params(limit=0)
        with self.assertRaisesRegex(ValueError, "cannot exceed 100"):
            MoySkladClient.build_query_params(expand=["positions"], limit=101)
        with self.assertRaisesRegex(ValueError, "non-negative"):
            MoySkladClient.build_query_params(offset=-1)
        with self.assertRaisesRegex(ValueError, "specified more than once"):
            MoySkladClient.build_query_params(params={"limit": 10}, limit=20)


class MoySkladClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_bearer_auth_and_does_not_log_token(self):
        seen_authorization = []
        seen_accept = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen_authorization.append(request.headers.get("Authorization"))
            seen_accept.append(request.headers.get("Accept"))
            return httpx.Response(200, json={"ok": True})

        token = "top-secret-token"
        client = MoySkladClient(
            token=token,
            base_url="https://example.test/api/remap/1.2",
            transport=httpx.MockTransport(handler),
        )

        with self.assertLogs("settings.moy_sklad", level="INFO") as captured:
            async with client:
                result = await client.request(
                    "GET",
                    "entity/product",
                    filters=["name=Chair"],
                )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(seen_authorization, [f"Bearer {token}"])
        self.assertEqual(seen_accept, ["application/json;charset=utf-8"])
        self.assertNotIn(token, "\n".join(captured.output))
        self.assertNotIn("Authorization", "\n".join(captured.output))

    async def test_generate_token_uses_basic_auth_and_retries_service_post(self):
        calls = 0
        authorization_headers = []
        accept_headers = []

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            authorization_headers.append(request.headers.get("Authorization"))
            accept_headers.append(request.headers.get("Accept"))
            if calls == 1:
                return httpx.Response(502, json={"errors": [{"error": "gateway"}]})
            return httpx.Response(200, json={"access_token": "new-token"})

        client = MoySkladClient(
            base_url="https://example.test/api/remap/1.2",
            backoff_factor=0,
            transport=httpx.MockTransport(handler),
        )
        with patch("settings.moy_sklad.asyncio.sleep", new=AsyncMock()) as sleep:
            async with client:
                token = await client.generate_token("user", "password")

        expected_basic = base64.b64encode(b"user:password").decode()
        self.assertEqual(token, "new-token")
        self.assertEqual(client.token, "new-token")
        self.assertEqual(calls, 2)
        self.assertEqual(authorization_headers, [f"Basic {expected_basic}"] * 2)
        self.assertEqual(
            accept_headers,
            ["application/json;charset=utf-8"] * 2,
        )
        sleep.assert_awaited_once_with(0)

    async def test_uses_moysklad_retry_after_header_for_429(self):
        calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(
                    429,
                    headers={"X-Lognex-Retry-After": "25"},
                    json={"errors": [{"error": "rate limit"}]},
                )
            return httpx.Response(200, json={"ok": True})

        client = MoySkladClient(
            token="token",
            base_url="https://example.test/api/remap/1.2",
            transport=httpx.MockTransport(handler),
        )
        with patch("settings.moy_sklad.asyncio.sleep", new=AsyncMock()) as sleep:
            result = await client.request("GET", "entity/product")
        await client.close()

        self.assertEqual(result, {"ok": True})
        self.assertEqual(calls, 2)
        sleep.assert_awaited_once_with(0.025)

    async def test_exhausts_retries_and_parses_api_error(self):
        calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(
                503,
                headers={"X-Lognex-Request-Id": "request-42"},
                json={"errors": [{"error": "temporarily unavailable", "code": 1}]},
            )

        client = MoySkladClient(
            token="token",
            base_url="https://example.test/api/remap/1.2",
            max_retries=2,
            backoff_factor=0,
            transport=httpx.MockTransport(handler),
        )
        with patch("settings.moy_sklad.asyncio.sleep", new=AsyncMock()) as sleep:
            with self.assertRaises(MoySkladAPIError) as raised:
                await client.request("GET", "entity/product")
        await client.close()

        self.assertEqual(calls, 3)
        self.assertEqual(sleep.await_count, 2)
        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(raised.exception.request_id, "request-42")
        self.assertEqual(raised.exception.errors[0]["code"], 1)
        self.assertIn("temporarily unavailable", str(raised.exception))

    async def test_does_not_retry_generic_post(self):
        calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(503, json={"errors": [{"error": "gateway"}]})

        client = MoySkladClient(
            token="token",
            base_url="https://example.test/api/remap/1.2",
            transport=httpx.MockTransport(handler),
        )
        with self.assertRaises(MoySkladAPIError):
            await client.request("POST", "entity/product", json={"name": "Chair"})
        await client.close()

        self.assertEqual(calls, 1)

    async def test_reports_non_json_success_response(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text="not-json")

        client = MoySkladClient(
            token="token",
            base_url="https://example.test/api/remap/1.2",
            transport=httpx.MockTransport(handler),
        )
        with self.assertRaisesRegex(MoySkladAPIError, "non-JSON"):
            await client.request("GET", "entity/product")
        await client.close()

    async def test_rejects_absolute_url_on_another_host(self):
        client = MoySkladClient(
            token="token",
            base_url="https://example.test/api/remap/1.2",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={"ok": True})
            ),
        )
        with self.assertRaisesRegex(ValueError, "external host"):
            await client.request("GET", "https://attacker.test/entity/product")
        await client.close()

    async def test_iter_rows_builds_queries_and_advances_offset(self):
        seen_queries = []

        def handler(request: httpx.Request) -> httpx.Response:
            query = dict(request.url.params)
            seen_queries.append(query)
            offset = int(query["offset"])
            if offset == 5:
                return httpx.Response(
                    200,
                    json={
                        "rows": [{"id": "1"}, {"id": "2"}],
                        "meta": {"nextHref": "https://example.test/next"},
                    },
                )
            return httpx.Response(200, json={"rows": [{"id": "3"}], "meta": {}})

        client = MoySkladClient(
            token="token",
            base_url="https://example.test/api/remap/1.2",
            transport=httpx.MockTransport(handler),
        )
        rows = [
            row
            async for row in client.iter_rows(
                "entity/processingorder",
                filters=["applicable=true"],
                expand=["positions"],
                order=["updated,desc"],
                limit=2,
                offset=5,
            )
        ]
        await client.close()

        self.assertEqual([row["id"] for row in rows], ["1", "2", "3"])
        self.assertEqual([query["offset"] for query in seen_queries], ["5", "7"])
        self.assertTrue(all(query["limit"] == "2" for query in seen_queries))
        self.assertTrue(all(query["filter"] == "applicable=true" for query in seen_queries))
        self.assertTrue(all(query["expand"] == "positions" for query in seen_queries))
        self.assertTrue(all(query["order"] == "updated,desc" for query in seen_queries))

    async def test_fetches_processing_order_and_all_expanded_positions(self):
        seen_queries = []

        def handler(request: httpx.Request) -> httpx.Response:
            query = dict(request.url.params)
            seen_queries.append((request.url.path, query))
            if request.url.path.endswith("/entity/processingorder/order-id"):
                return httpx.Response(
                    200,
                    json={
                        "id": "order-id",
                        "name": "Order 1",
                        "meta": {"type": "processingorder"},
                        "positions": {
                            "meta": {
                                "href": "https://example.test/api/remap/1.2/"
                                "entity/processingorder/order-id/positions"
                            }
                        },
                    },
                )

            offset = int(query["offset"])
            count = 100 if offset == 0 else 1
            return httpx.Response(
                200,
                json={
                    "rows": [
                        {"id": f"position-{offset + index}"}
                        for index in range(count)
                    ],
                    "meta": {
                        "nextHref": "next" if offset == 0 else None,
                    },
                },
            )

        client = MoySkladClient(
            token="token",
            base_url="https://example.test/api/remap/1.2",
            transport=httpx.MockTransport(handler),
        )
        order, positions = await client.fetch_processing_order(
            "https://example.test/api/remap/1.2/entity/processingorder/order-id"
        )
        await client.close()

        self.assertEqual(order["id"], "order-id")
        self.assertEqual(len(positions), 101)
        self.assertEqual(
            seen_queries[0][1]["expand"],
            "state,processingPlan",
        )
        self.assertEqual(seen_queries[1][1]["expand"], "assortment")
        self.assertEqual(seen_queries[1][1]["limit"], "100")
        self.assertEqual(seen_queries[2][1]["offset"], "100")

    async def test_fetches_order_edit_catalogs_and_updates_processing_order(self):
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(
                (
                    request.method,
                    request.url.path,
                    dict(request.url.params),
                    json.loads(request.content) if request.content else None,
                )
            )
            if request.method == "PUT":
                return httpx.Response(
                    200,
                    json={"id": "order-id", "quantity": 7},
                )
            return httpx.Response(
                200,
                json={
                    "rows": [
                        {
                            "id": "row-id",
                            "name": "Row",
                            "meta": {"href": str(request.url), "type": "entity"},
                        }
                    ],
                    "meta": {},
                },
            )

        client = MoySkladClient(
            token="token",
            base_url="https://example.test/api/remap/1.2",
            transport=httpx.MockTransport(handler),
        )
        attributes = await client.fetch_processing_order_attributes()
        employees = await client.fetch_active_employees()
        plans = await client.fetch_active_processing_plans()
        devices = await client.fetch_custom_entity_rows(
            "https://example.test/api/remap/1.2/entity/customentity/devices/metadata"
        )
        updated = await client.update_processing_order(
            "order-id",
            {"quantity": 7},
        )
        await client.close()

        self.assertEqual(attributes[0]["id"], "row-id")
        self.assertEqual(employees[0]["id"], "row-id")
        self.assertEqual(plans[0]["id"], "row-id")
        self.assertEqual(devices[0]["id"], "row-id")
        self.assertEqual(updated["quantity"], 7)
        self.assertEqual(
            [request[1] for request in requests],
            [
                "/api/remap/1.2/entity/processingorder/metadata/attributes",
                "/api/remap/1.2/entity/employee",
                "/api/remap/1.2/entity/processingplan",
                "/api/remap/1.2/entity/customentity/devices",
                "/api/remap/1.2/entity/processingorder/order-id",
            ],
        )
        self.assertEqual(requests[1][2]["filter"], "archived=false")
        self.assertEqual(requests[1][2]["order"], "name,asc")
        self.assertEqual(requests[2][2]["filter"], "archived=false")
        self.assertEqual(requests[4][0], "PUT")
        self.assertEqual(requests[4][2]["expand"], "processingPlan")
        self.assertEqual(requests[4][3], {"quantity": 7})

    async def test_accepts_custom_entity_metadata_href_variants(self):
        paths = []

        def handler(request: httpx.Request) -> httpx.Response:
            paths.append(request.url.path)
            return httpx.Response(200, json={"rows": [], "meta": {}})

        client = MoySkladClient(
            token="token",
            base_url="https://example.test/api/remap/1.2",
            transport=httpx.MockTransport(handler),
        )
        for href in (
            "https://example.test/api/remap/1.2/entity/customentity/devices/metadata/",
            "https://example.test/api/remap/1.2/entity/customentity/devices",
            "https://example.test/api/remap/1.2/entity/customentity/devices?expand=owner",
        ):
            self.assertEqual(await client.fetch_custom_entity_rows(href), [])
        await client.close()

        self.assertEqual(
            paths,
            ["/api/remap/1.2/entity/customentity/devices"] * 3,
        )

    async def test_rejects_invalid_custom_entity_metadata_href(self):
        client = MoySkladClient(
            token="token",
            base_url="https://example.test/api/remap/1.2",
        )
        with self.assertRaisesRegex(ValueError, "must identify a custom entity"):
            await client.fetch_custom_entity_rows(
                "https://example.test/api/remap/1.2/entity/product/device-id"
            )


class MoySkladWebhookTests(unittest.IsolatedAsyncioTestCase):
    async def test_creates_both_webhooks_once_and_then_reuses_them(self):
        rows = []
        post_payloads = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(200, json={"rows": list(rows), "meta": {}})
            if request.method == "POST":
                payload = json.loads(request.content)
                post_payloads.append(payload)
                created = {
                    **payload,
                    "id": f"webhook-{len(rows) + 1}",
                    "enabled": True,
                }
                rows.append(created)
                return httpx.Response(200, json=created)
            raise AssertionError(f"Unexpected request: {request.method}")

        client = MoySkladClient(
            token="token",
            base_url="https://example.test/api/remap/1.2",
            transport=httpx.MockTransport(handler),
        )
        first = await client.ensure_processing_order_webhooks("https://app.test/webhook")
        second = await client.ensure_processing_order_webhooks("https://app.test/webhook")
        await client.close()

        self.assertEqual([item["action"] for item in first], ["CREATE", "UPDATE"])
        self.assertEqual([item["id"] for item in second], ["webhook-1", "webhook-2"])
        self.assertEqual(len(post_payloads), 2)
        self.assertEqual(post_payloads[0]["entityType"], "processingorder")
        self.assertNotIn("diffType", post_payloads[0])
        self.assertEqual(post_payloads[1]["diffType"], "FIELDS")

    async def test_enables_existing_webhook_and_updates_diff_type(self):
        rows = [
            {
                "id": "create-id",
                "entityType": "processingorder",
                "action": "CREATE",
                "url": "https://app.test/webhook",
                "enabled": True,
            },
            {
                "id": "update-id",
                "entityType": "processingorder",
                "action": "UPDATE",
                "url": "https://app.test/webhook",
                "enabled": False,
                "diffType": "NONE",
            },
        ]
        put_payloads = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(200, json={"rows": rows, "meta": {}})
            if request.method == "PUT":
                payload = json.loads(request.content)
                put_payloads.append(payload)
                updated = {**rows[1], **payload}
                rows[1] = updated
                return httpx.Response(200, json=updated)
            raise AssertionError(f"Unexpected request: {request.method}")

        client = MoySkladClient(
            token="token",
            base_url="https://example.test/api/remap/1.2",
            transport=httpx.MockTransport(handler),
        )
        result = await client.ensure_processing_order_webhooks("https://app.test/webhook")
        await client.close()

        self.assertEqual(put_payloads, [{"enabled": True, "diffType": "FIELDS"}])
        self.assertTrue(result[1]["enabled"])
        self.assertEqual(result[1]["diffType"], "FIELDS")


class MoySkladEnvironmentTests(unittest.TestCase):
    def test_saves_token_to_selected_env_file(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = Path(directory) / ".env"
            env_path.write_text("EXISTING=value\n", encoding="utf-8")

            save_token_to_env("saved-token", env_path)

            values = dotenv.dotenv_values(env_path)
            self.assertEqual(values["EXISTING"], "value")
            self.assertEqual(values["MOYSKLAD_TOKEN"], "saved-token")


class MoySkladCLITests(unittest.IsolatedAsyncioTestCase):
    async def test_token_command_reads_credentials_and_saves_token(self):
        fake_client = AsyncMock()
        fake_client.__aenter__.return_value = fake_client
        fake_client.generate_token.return_value = "cli-token"

        with tempfile.TemporaryDirectory() as directory:
            env_path = Path(directory) / ".env"
            env_path.write_text(
                "MOYSKLAD_LOGIN=user\nMOYSKLAD_PASSWORD=password\n",
                encoding="utf-8",
            )

            with (
                patch.dict("os.environ", {}, clear=True),
                patch("settings.moy_sklad.MoySkladClient", return_value=fake_client),
                self.assertLogs("settings.moy_sklad", level="INFO") as captured,
            ):
                await _run_cli(SimpleNamespace(command="token"), env_path)

            values = dotenv.dotenv_values(env_path)

        fake_client.generate_token.assert_awaited_once_with("user", "password")
        self.assertEqual(values["MOYSKLAD_TOKEN"], "cli-token")
        self.assertNotIn("cli-token", "\n".join(captured.output))


if __name__ == "__main__":
    unittest.main()
