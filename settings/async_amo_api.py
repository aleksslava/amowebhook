import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import dotenv
import httpx
import jwt

logger = logging.getLogger(__name__)


# ====== твои dataclasses (оставлены как есть) ======

@dataclass
class AmoLead:
    lead_id: int
    lead_price: int | float | None
    created_at: int | None
    close_at: int | None
    contact_id: int | None
    shipment_at: str | int | None
    clean_price: int | float = 0
    last_buy: int | str | None = None
    time_from_attestate: int | str | None = None

    @property
    def price(self) -> int | float | None:
        return self.lead_price

    @price.setter
    def price(self, value: int | float | None) -> None:
        self.lead_price = value


@dataclass
class AmoContact:
    contact_id: int
    customer_id: int | None
    attestate_at: str | int | None


@dataclass
class AmoResult:
    lead_obj: AmoLead
    contact_obj: AmoContact

def build_amo_results(
    leads: list[AmoLead],
    contacts: list[AmoContact]
) -> list[AmoResult]:
    logger.info(f'Количество объектов Лид: {len(leads)}')
    logger.info(f'Количество объектов Контакт: {len(contacts)}')
    result: list[AmoResult] = []
    for lead_obj in leads:
        lead_contact_id = lead_obj.contact_id
        for contact_obj in contacts:
            if lead_contact_id == contact_obj.contact_id:
                result.append(AmoResult(lead_obj=lead_obj, contact_obj=contact_obj))
                continue
    # Откидываем лиды с пустым значением даты отгрузки и сортируем список сделок по дате отгрузки
    result = sorted(filter(lambda x: x.lead_obj.shipment_at != 0, result), key=lambda x: x.lead_obj.shipment_at)

    for index, record in enumerate(result):
        current_lead = record.lead_obj
        current_contact = record.contact_obj

        try:
            # Высчитываем поле "Времени с момента аттестации"
            if current_contact.attestate_at and current_lead.shipment_at and current_lead.shipment_at > current_contact.attestate_at:
                current_lead.time_from_attestate = current_lead.shipment_at - current_contact.attestate_at
            else:
                current_lead.time_from_attestate = None
        except BaseException as error:
            current_lead.time_from_attestate = None
            logger.error(error)

        # Считаем поле "Чистый выкуп до текущей покупки и дату прошлой покупки
        if index != 0:
            records_by_contact = list(filter(lambda x: x.contact_obj.customer_id == current_contact.customer_id, result[:index]))
            if records_by_contact:
                clean_price = sum(record.lead_obj.price for record in records_by_contact)
                current_lead.clean_price = clean_price
                try:
                    if current_lead.shipment_at and records_by_contact[-1].lead_obj.shipment_at:
                        current_lead.last_buy = current_lead.shipment_at - records_by_contact[-1].lead_obj.shipment_at
                    else:
                        current_lead.last_buy = 0
                except BaseException as error:
                    current_lead.last_buy = 0
                    logger.error(error)
                    logger.error(current_lead.shipment_at, records_by_contact[-1].lead_obj.shipment_at)

            else:
                current_lead.clean_price = 0
                current_lead.last_buy = 0

    return result


# ====== async wrapper ======

class AmoCRMWrapperAsync:
    """
    Async-версия AmoCRMWrapper на httpx.AsyncClient.

    Использование:
        async with AmoCRMWrapperAsync(...) as amo:
            await amo.init_oauth2()
            contacts = await amo.get_contacts_with_customer()
    """

    def __init__(
        self,
        path: str,
        amocrm_subdomain: str,
        amocrm_client_id: str,
        amocrm_client_secret: str,
        amocrm_redirect_url: str,
        amocrm_access_token: str | None,
        amocrm_refresh_token: str | None,
        amocrm_secret_code: str,
        *,
        timeout: float = 30.0,
        min_delay_seconds: float = 0.2,  # как у тебя в коде
        max_retries: int = 2,
    ):
        self.path_to_env = path
        self.amocrm_subdomain = amocrm_subdomain
        self.amocrm_client_id = amocrm_client_id
        self.amocrm_client_secret = amocrm_client_secret
        self.amocrm_redirect_url = amocrm_redirect_url
        self.amocrm_access_token = amocrm_access_token
        self.amocrm_refresh_token = amocrm_refresh_token
        self.amocrm_secret_code = amocrm_secret_code

        self._base_url = f"https://{self.amocrm_subdomain}.amocrm.ru"
        self._timeout = httpx.Timeout(timeout)
        self._client: Optional[httpx.AsyncClient] = None

        self._token_lock = asyncio.Lock()
        self._rate_lock = asyncio.Lock()
        self._min_delay_seconds = float(min_delay_seconds)
        self._last_request_ts: float = 0.0

        self._max_retries = int(max_retries)

    async def __aenter__(self) -> "AmoCRMWrapperAsync":
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def open(self) -> None:
        """
        Явно инициализирует httpx.AsyncClient.
        Можно вызывать несколько раз — повторно клиент не создастся.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self._base_url, timeout=self._timeout)

    async def close(self) -> None:
        """
        Явно закрывает httpx.AsyncClient.
        Безопасно при повторных вызовах.
        """
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ---------- token helpers ----------

    @staticmethod
    def _is_expire(token: str) -> bool:
        try:
            token_data = jwt.decode(token, options={"verify_signature": False})
            exp = datetime.utcfromtimestamp(token_data["exp"])
            return datetime.utcnow() >= exp
        except Exception:
            # если токен битый/пустой — считаем истёкшим
            return True

    def _save_tokens(self, access_token: str, refresh_token: str) -> None:
        dotenv.set_key(self.path_to_env, "AMOCRM_ACCESS_TOKEN", access_token)
        dotenv.set_key(self.path_to_env, "AMOCRM_REFRESH_TOKEN", refresh_token)
        self.amocrm_access_token = access_token
        self.amocrm_refresh_token = refresh_token

    def _get_access_token(self) -> str:
        if not self.amocrm_access_token:
            raise RuntimeError("AMOCRM_ACCESS_TOKEN is empty")
        return self.amocrm_access_token

    async def _throttle(self) -> None:
        """
        Гарантирует min_delay_seconds между запросами (не блокирует event loop).
        """
        async with self._rate_lock:
            now = asyncio.get_running_loop().time()
            wait_for = (self._last_request_ts + self._min_delay_seconds) - now
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            self._last_request_ts = asyncio.get_running_loop().time()

    async def _get_new_tokens(self) -> bool:
        if not self.amocrm_refresh_token:
            logger.error("AMOCRM_REFRESH_TOKEN is empty")
            return False

        assert self._client is not None, "Client is not initialized. Use 'async with'."

        data = {
            "client_id": self.amocrm_client_id,
            "client_secret": self.amocrm_client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.amocrm_refresh_token,
            "redirect_uri": self.amocrm_redirect_url,
        }

        await self._throttle()
        resp = await self._client.post("/oauth2/access_token", json=data)

        try:
            payload = resp.json()
        except Exception:
            logger.error("Ошибка JSON при обновлении токенов: %s", resp.text)
            return False

        access_token = payload.get("access_token")
        refresh_token = payload.get("refresh_token")
        if not access_token or not refresh_token:
            logger.error("Ошибка обновления токенов: %s", payload)
            return False

        self._save_tokens(access_token, refresh_token)
        return True

    async def _ensure_token(self) -> None:
        token = self.amocrm_access_token
        if not token:
            raise RuntimeError("Access token is not set. Call init_oauth2() or provide token.")

        if not self._is_expire(token):
            return

        async with self._token_lock:
            # double-check после ожидания lock
            token2 = self.amocrm_access_token
            if token2 and not self._is_expire(token2):
                return

            ok = await self._get_new_tokens()
            if not ok:
                raise RuntimeError("Failed to refresh AmoCRM tokens")

    async def init_oauth2(self) -> dict[str, Any]:
        """
        Первичная авторизация по authorization_code (secret_code).
        """
        assert self._client is not None, "Client is not initialized. Use 'async with'."

        data = {
            "client_id": self.amocrm_client_id,
            "client_secret": self.amocrm_client_secret,
            "grant_type": "authorization_code",
            "code": self.amocrm_secret_code,
            "redirect_uri": self.amocrm_redirect_url,
        }

        await self._throttle()
        resp = await self._client.post("/oauth2/access_token", json=data)
        payload = resp.json()

        access_token = payload["access_token"]
        refresh_token = payload["refresh_token"]
        self._save_tokens(access_token, refresh_token)
        return payload

    # ---------- request core ----------

    async def _request_with_retries(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        json_data: Any = None,
    ) -> httpx.Response:
        assert self._client is not None, "Client is not initialized. Use 'async with'."

        last_exc: Exception | None = None

        for attempt in range(self._max_retries + 1):
            try:
                await self._throttle()
                resp = await self._client.request(method, url, headers=headers, json=json_data)

                # простой retry на 429/5xx
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < self._max_retries:
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        await asyncio.sleep(int(retry_after))
                    else:
                        await asyncio.sleep(0.5 * (attempt + 1))
                    continue

                return resp

            except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                raise

        # формально недостижимо
        raise RuntimeError(f"Request failed: {last_exc!r}")

    async def _base_request(self, **kwargs) -> httpx.Response:
        """
        Совместимо с твоей сигнатурой: type, endpoint, parameters, data.
        Возвращает httpx.Response.
        """
        await self._ensure_token()

        access_token = "Bearer " + self._get_access_token()
        headers = {"Authorization": access_token}

        req_type = kwargs.get("type")
        endpoint = kwargs.get("endpoint")
        parameters = kwargs.get("parameters")
        data = kwargs.get("data")

        if not endpoint:
            raise ValueError("endpoint is required")

        if req_type == "get":
            return await self._request_with_retries("GET", endpoint, headers=headers)

        if req_type == "get_param":
            url = f"{endpoint}?{parameters}" if parameters else endpoint
            return await self._request_with_retries("GET", url, headers=headers)

        if req_type == "post":
            return await self._request_with_retries("POST", endpoint, headers=headers, json_data=data)

        if req_type == "patch":
            return await self._request_with_retries("PATCH", endpoint, headers=headers, json_data=data)

        raise ValueError(f"Unknown request type: {req_type}")

    # ---------- бизнес-методы (async) ----------

    async def get_contact_by_phone(self, phone_number, with_customer: bool = False) -> tuple[bool, Any]:
        phone_number = str(phone_number)[2:]
        url = "/api/v4/contacts"
        query = f"query={phone_number}&with=customers" if with_customer else f"query={phone_number}"

        resp = await self._base_request(endpoint=url, type="get_param", parameters=query)

        if resp.status_code == 200:
            contacts_list = resp.json()["_embedded"]["contacts"]
            if len(contacts_list) > 1:
                return False, (
                    "Найдено более одного контакта с номером телефона\n"
                    "Обратитесь к менеджеру отдела продаж!"
                )
            return True, contacts_list[0]

        if resp.status_code == 204:
            return False, "Контакт не найден"

        logger.error("AMO_API error: %s %s", resp.status_code, resp.text)
        return False, "Произошла ошибка на сервере!"

    @staticmethod
    def _get_customer_id_from_contact(contact_data: dict) -> int | None:
        embedded_data = contact_data.get("_embedded", {})

        customers = embedded_data.get("customers", [])
        if customers:
            return customers[0].get("id")

        customer = embedded_data.get("customer")
        if isinstance(customer, dict):
            return customer.get("id")
        if isinstance(customer, list) and customer:
            return customer[0].get("id")

        return None

    def _get_contacts_embedded_list(self, payload: dict) -> list[dict]:
        return payload.get("_embedded", {}).get("contacts", []) or []

    async def get_contacts_with_customer(self, limit: int = 250) -> list[AmoContact]:
        url = "/api/v4/contacts"
        page = 1
        all_contacts: list[AmoContact] = []
        attestate_field_id = 1096322

        while True:
            logger.info("Запрос контактов, страница: %s", page)
            query = f"with=customers&limit={limit}&page={page}"
            resp = await self._base_request(endpoint=url, type="get_param", parameters=query)

            if resp.status_code == 204:
                break

            if resp.status_code != 200:
                logger.error(
                    "Не удалось получить контакты: status_code=%s, page=%s, body=%s",
                    resp.status_code,
                    page,
                    resp.text,
                )
                break

            page_items = self._get_contacts_embedded_list(resp.json())
            if not page_items:
                break

            for contact in page_items:
                all_contacts.append(
                    AmoContact(
                        contact_id=contact.get("id"),
                        customer_id=self._get_customer_id_from_contact(contact),
                        attestate_at=self._get_custom_field_value(contact, attestate_field_id),
                    )
                )

            if len(page_items) < limit:
                break

            page += 1

        return all_contacts

    async def add_new_task(self, contact_id, descr, url_materials, time_value) -> httpx.Response:
        url = "/api/v4/tasks"
        data = [
            {
                "text": f"Обращение по ошибке чат-бота:\n{descr} {url_materials}",
                "complete_till": int(time_value),
                "entity_id": contact_id,
                "entity_type": "contacts",
                "responsible_user_id": 6390936,
            },
            {
                "text": f"Обращение по ошибке чат-бота:\n{descr} {url_materials}",
                "complete_till": int(time_value),
                "entity_id": contact_id,
                "entity_type": "contacts",
                "responsible_user_id": 10353813,
            },
        ]
        return await self._base_request(type="post", endpoint=url, data=data)

    @staticmethod
    def _get_main_contact_id(lead_data: dict) -> int | None:
        contacts = lead_data.get("_embedded", {}).get("contacts", [])
        if not contacts:
            return None
        for contact in contacts:
            if contact.get("is_main"):
                return contact.get("id")
        return contacts[0].get("id")

    @staticmethod
    def _get_custom_field_value(lead_data: dict, field_id: int):
        custom_fields = lead_data.get("custom_fields_values", [])
        if custom_fields is not None:
            for field in custom_fields:
                if field.get("field_id") == field_id:
                    values = field.get("values", [])
                    if values:
                        return values[0].get("value")
        return 0

    @staticmethod
    def _convert_unix_to_sheets_datetime(timestamp_value):
        if timestamp_value in (None, ""):
            return None
        try:
            unix_timestamp = float(timestamp_value)
        except (TypeError, ValueError):
            return timestamp_value
        if unix_timestamp > 10_000_000_000:
            unix_timestamp /= 1000
        try:
            dt_value = datetime.fromtimestamp(unix_timestamp)
        except (OverflowError, OSError, ValueError):
            return timestamp_value
        return dt_value.strftime("%d-%m-%Y %H:%M:%S")

    async def get_pipeline_1628622_status_142_leads(self, limit: int = 250) -> list[AmoLead]:
        url = "/api/v4/leads"
        page = 1
        all_leads: list[AmoLead] = []
        shipment_field_id = 935651

        while True:
            logger.info("Запрос сделок, страница: %s", page)
            query = (
                "filter[pipeline_id][]=1628622&"
                "filter[statuses][0][pipeline_id]=1628622&"
                "filter[statuses][0][status_id]=142&"
                "with=contacts&"
                f"limit={limit}&"
                f"page={page}"
            )

            resp = await self._base_request(endpoint=url, type="get_param", parameters=query)

            if resp.status_code == 204:
                break

            if resp.status_code != 200:
                logger.error(
                    "Не удалось получить сделки: status_code=%s, page=%s, body=%s",
                    resp.status_code,
                    page,
                    resp.text,
                )
                break

            page_items = resp.json().get("_embedded", {}).get("leads", []) or []
            if not page_items:
                break

            for lead in page_items:
                all_leads.append(
                    AmoLead(
                        lead_id=lead.get("id"),
                        lead_price=lead.get("price"),
                        created_at=lead.get("created_at"),
                        close_at=lead.get("closed_at"),
                        contact_id=self._get_main_contact_id(lead),
                        shipment_at=self._get_custom_field_value(lead, shipment_field_id),
                    )
                )

            if len(page_items) < limit:
                break

            page += 1

        return all_leads

    async def add_catalog_elements_to_lead(self, lead_id, elements) -> dict:
        url = f"/api/v4/leads/{lead_id}/link"
        data = []
        for element in elements:
            element_id = int(element.get("id"))
            quantity = int(float(element.get("quantity")))
            data.append(
                {
                    "to_entity_id": element_id,
                    "to_entity_type": "catalog_elements",
                    "metadata": {"quantity": quantity, "catalog_id": 1682},
                }
            )
        resp = await self._base_request(type="post", endpoint=url, data=data)
        return resp.json()

    async def create_new_contact(self, first_name: str, last_name: str, phone: str) -> int:
        url = "/api/v4/contacts"
        data = [
            {
                "first_name": first_name,
                "last_name": last_name,
                "responsible_user_id": 11047749,
                "custom_fields_values": [
                    {
                        "field_id": 671750,
                        "values": [{"enum_code": "WORK", "value": str(phone)}],
                    }
                ],
            }
        ]
        resp = await self._base_request(type="post", endpoint=url, data=data)
        contact_id = resp.json().get("_embedded", {}).get("contacts", [{}])[0].get("id")
        return int(contact_id)

    async def send_lead_to_amo(self, contact_id: int, order_id: str) -> dict:
        url = "/api/v4/leads"
        data = [
            {
                "name": "Заказ с маркета",
                "pipeline_id": 25020,
                "created_by": 0,
                "status_id": 17566048,
                "responsible_user_id": 11047749,
                "custom_fields_values": [
                    {"field_id": 1101072, "values": [{"value": str(order_id)}]}
                ],
                "_embedded": {
                    "tags": [{"id": 563936}],
                    "contacts": [{"id": contact_id}],
                },
            }
        ]
        resp = await self._base_request(type="post", endpoint=url, data=data)
        return resp.json()

    async def add_new_note_to_lead(self, lead_id, text, order_id) -> dict:
        url = f"/api/v4/leads/{lead_id}/notes"
        market_order_url = f"https://partner.market.yandex.ru/order/{order_id}?partnerId=182087723"
        _ = market_order_url  # оставлено для совместимости (в твоём коде не использовалось)
        data = [{"note_type": "common", "params": {"text": text}}]
        resp = await self._base_request(type="post", endpoint=url, data=data)
        return resp.json()

    async def get_lead_by_id(self, lead_id) -> dict:
        url = f"/api/v4/leads/{lead_id}"
        resp = await self._base_request(type="get", endpoint=url)
        return resp.json()

    async def get_contact_by_id(self, contact_id) -> dict:
        url = f"/api/v4/contacts/{contact_id}"
        resp = await self._base_request(type="get", endpoint=url)
        return resp.json()


# ====== пример запуска ======
# if __name__ == "__main__":
#     from settings import load_config, Config
#
#     async def main():
#         config: Config = load_config(path="../.env")
#         async with AmoCRMWrapperAsync(
#             path=config.amo_config.path_to_env,
#             amocrm_subdomain=config.amo_config.amocrm_subdomain,
#             amocrm_client_id=config.amo_config.amocrm_client_id,
#             amocrm_redirect_url=config.amo_config.amocrm_redirect_url,
#             amocrm_client_secret=config.amo_config.amocrm_client_secret,
#             amocrm_secret_code=config.amo_config.amocrm_secret_code,
#             amocrm_access_token=config.amo_config.amocrm_access_token,
#             amocrm_refresh_token=config.amo_config.amocrm_refresh_token,
#         ) as amo:
#             await amo.init_oauth2()
#             contacts = await amo.get_contacts_with_customer()
#             leads = await amo.get_pipeline_1628622_status_142_leads()
#             print(len(contacts), len(leads))
#
#     asyncio.run(main())