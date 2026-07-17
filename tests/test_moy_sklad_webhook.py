import unittest
from unittest.mock import AsyncMock, patch

import httpx
from sqlalchemy.exc import SQLAlchemyError

import main
from services.moy_sklad_sync import MoySkladWebhookPayloadError
from settings.moy_sklad import MoySkladAPIError


class MoySkladWebhookEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        transport = httpx.ASGITransport(app=main.app)
        self.client = httpx.AsyncClient(
            transport=transport,
            base_url="https://example.test",
        )

    async def asyncTearDown(self) -> None:
        await self.client.aclose()

    async def test_logs_payload_and_returns_ok(self):
        payload = {
            "events": [
                {
                    "action": "CREATE",
                    "meta": {
                        "href": "https://api.moysklad.ru/api/remap/1.2/"
                        "entity/processingorder/order-id",
                        "type": "processingorder",
                    },
                },
                {
                    "action": "UPDATE",
                    "meta": {
                        "href": "https://api.moysklad.ru/api/remap/1.2/"
                        "entity/processingorder/order-id",
                        "type": "processingorder",
                    },
                },
            ]
        }

        processor = AsyncMock(return_value=[])
        with (
            patch.object(main.config, "moysklad_token", "token"),
            patch("main.process_processing_order_webhook", new=processor),
            patch.object(main.logger, "info") as log,
        ):
            response = await self.client.post("/moysklad/processingorder", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})
        log.assert_called_once_with(
            "MoySklad processingorder webhook payload=%s",
            payload,
        )
        processor.assert_awaited_once_with(
            payload,
            main.moysklad_client,
            main.SessionLocal,
        )

    async def test_returns_503_when_token_is_missing(self):
        processor = AsyncMock()
        with (
            patch.object(main.config, "moysklad_token", None),
            patch("main.process_processing_order_webhook", new=processor),
        ):
            response = await self.client.post(
                "/moysklad/processingorder",
                json={"events": []},
            )

        self.assertEqual(response.status_code, 503)
        processor.assert_not_awaited()

    async def test_maps_payload_api_and_database_errors(self):
        errors = [
            (MoySkladWebhookPayloadError("invalid events"), 400),
            (
                MoySkladAPIError(
                    status_code=503,
                    method="GET",
                    endpoint="entity/processingorder/order-id",
                    errors=[{"error": "unavailable"}],
                ),
                502,
            ),
            (SQLAlchemyError("database failed"), 500),
        ]
        for error, expected_status in errors:
            with self.subTest(expected_status=expected_status):
                processor = AsyncMock(side_effect=error)
                with (
                    patch.object(main.config, "moysklad_token", "token"),
                    patch("main.process_processing_order_webhook", new=processor),
                ):
                    response = await self.client.post(
                        "/moysklad/processingorder",
                        json={"events": []},
                    )
                self.assertEqual(response.status_code, expected_status)

    async def test_rejects_invalid_json_without_logging(self):
        with patch.object(main.logger, "info") as log:
            response = await self.client.post(
                "/moysklad/processingorder",
                content="{",
                headers={"Content-Type": "application/json"},
            )

        self.assertEqual(response.status_code, 422)
        log.assert_not_called()

    async def test_does_not_accept_get(self):
        response = await self.client.get("/moysklad/processingorder")

        self.assertEqual(response.status_code, 405)


if __name__ == "__main__":
    unittest.main()
