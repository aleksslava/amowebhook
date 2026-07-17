import unittest
from unittest.mock import patch

import httpx

from main import app, logger


class MoySkladWebhookEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        transport = httpx.ASGITransport(app=app)
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

        with patch.object(logger, "info") as log:
            response = await self.client.post(
                "/moysklad/processingorder",
                json=payload,
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})
        log.assert_called_once_with(
            "MoySklad processingorder webhook payload=%s",
            payload,
        )

    async def test_rejects_invalid_json_without_logging(self):
        with patch.object(logger, "info") as log:
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
