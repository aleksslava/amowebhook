import logging
from typing import Any

import requests


logger = logging.getLogger(__name__)


class GoogleSheetsIntegration:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send_json(self, payload: list[dict[str, Any]], token: str, request_id: str) -> requests.Response:
        response = requests.post(
            self.webhook_url,
            params={'token': token, 'request_id': request_id},
            json=payload,
            timeout=30
        )
        if response.status_code >= 400:
            logger.error(
                f'Ошибка отправки данных в Google Sheets: status_code={response.status_code}, body={response.text}'
            )
        return response
