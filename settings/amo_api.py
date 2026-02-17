import pprint
from logging import Filter
from dataclasses import dataclass

import dotenv
import jwt
import requests
from datetime import datetime, timedelta
import time
import logging

from pydantic import json
from requests.exceptions import JSONDecodeError

logger = logging.getLogger(__name__)


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
    time_from_attestate: int | str | None = None


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
    result = sorted(result, key=lambda x: x.lead_obj.created_at)
    for index, record in enumerate(result):
        current_lead = record.lead_obj
        current_contact = record.contact_obj

        # Высчитываем поле "Времени с момента аттестации"
        if current_contact.attestate_at is not None and current_lead.created_at is not None and current_lead.created_at > current_contact.attestate_at:
            current_contact.time_from_attestate = current_lead.created_at - current_contact.attestate_at
        else:
            current_contact.time_from_attestate = None

        # Считаем поле "Чистый выкуп до текущей покупки и дату прошлой покупки
        if index != 0:
            records_by_contact = list(filter(lambda x: x.contact_obj.customer_id == current_contact.customer_id, result[:index]))
            if records_by_contact:
                clean_price = sum(record.lead_obj.price for record in records_by_contact)
                current_lead.clean_price = clean_price
                try:
                    if current_lead.shipment_at is not None and records_by_contact[-1].lead_obj.shipment_at is not None:
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



class AmoCRMWrapper:
    def __init__(self,
                 path: str,
                 amocrm_subdomain: str,
                 amocrm_client_id: str,
                 amocrm_client_secret: str,
                 amocrm_redirect_url: str,
                 amocrm_access_token: str | None,
                 amocrm_refresh_token: str | None,
                 amocrm_secret_code: str
                 ):
        self.path_to_env = path
        self.amocrm_subdomain = amocrm_subdomain
        self.amocrm_client_id = amocrm_client_id
        self.amocrm_client_secret = amocrm_client_secret
        self.amocrm_redirect_url = amocrm_redirect_url
        self.amocrm_access_token = amocrm_access_token
        self.amocrm_refresh_token = amocrm_refresh_token
        self.amocrm_secret_code = amocrm_secret_code


    @staticmethod
    def _is_expire(token: str):
        token_data = jwt.decode(token, options={"verify_signature": False})
        exp = datetime.utcfromtimestamp(token_data["exp"])
        now = datetime.utcnow()

        return now >= exp

    def _save_tokens(self, access_token: str, refresh_token: str):
        dotenv.set_key(self.path_to_env, "AMOCRM_ACCESS_TOKEN", access_token)
        dotenv.set_key(self.path_to_env, "AMOCRM_REFRESH_TOKEN", refresh_token)
        self.amocrm_access_token = access_token
        self.amocrm_refresh_token = refresh_token

    def _get_access_token(self):
        return self.amocrm_access_token

    def _get_new_tokens(self):
        data = {
            "client_id": self.amocrm_client_id,
            "client_secret": self.amocrm_client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.amocrm_refresh_token,
            "redirect_uri": self.amocrm_redirect_url
        }
        response = requests.post("https://{}.amocrm.ru/oauth2/access_token".format(self.amocrm_subdomain),
                                 json=data).json()
        try:
            access_token = response["access_token"]
            refresh_token = response["refresh_token"]
        except KeyError as error:
            logger.error("Ошибка обновления токенов")
            return False

        self._save_tokens(access_token, refresh_token)

    def init_oauth2(self):
        data = {
            "client_id": self.amocrm_client_id,
            "client_secret": self.amocrm_client_secret,
            "grant_type": "authorization_code",
            "code": self.amocrm_secret_code,
            "redirect_uri": self.amocrm_redirect_url
        }

        response = requests.post("https://{}.amocrm.ru/oauth2/access_token".format(self.amocrm_subdomain),
                                 json=data).json()
        print(response)

        access_token = response["access_token"]
        refresh_token = response["refresh_token"]

        self._save_tokens(access_token, refresh_token)

    def _base_request(self, **kwargs) -> json:
        if self._is_expire(self._get_access_token()):
            self._get_new_tokens()

        access_token = "Bearer " + self._get_access_token()

        headers = {"Authorization": access_token}
        req_type = kwargs.get("type")
        response = ""
        if req_type == "get":
            response = requests.get("https://{}.amocrm.ru{}".format(
                self.amocrm_subdomain, kwargs.get("endpoint")), headers=headers)

        elif req_type == "get_param":
            url = "https://{}.amocrm.ru{}?{}".format(
                self.amocrm_subdomain,
                kwargs.get("endpoint"), kwargs.get("parameters"))
            response = requests.get(str(url), headers=headers)

        elif req_type == "post":
            response = requests.post("https://{}.amocrm.ru{}".format(
                self.amocrm_subdomain,
                kwargs.get("endpoint")), headers=headers, json=kwargs.get("data"))

        elif req_type == 'patch':
            response = requests.patch("https://{}.amocrm.ru{}".format(
                self.amocrm_subdomain,
                kwargs.get("endpoint")), headers=headers, json=kwargs.get("data"))
        return response

    def get_contact_by_phone(self, phone_number, with_customer=False) -> tuple:
        phone_number = str(phone_number)[2:]
        url = '/api/v4/contacts'
        if with_customer:
            query = str(f'query={phone_number}&with=customers')
        else:
            query = str(f'query={phone_number}')
        contact = self._base_request(endpoint=url, type="get_param", parameters=query)
        if contact.status_code == 200:
            contacts_list = contact.json()['_embedded']['contacts']
            if len(contacts_list) > 1:  # Проверка на дубли номера телефона в контактах
                return False, ('Найдено более одного контакта с номером телефона\n'
                               'Обратитесь к менеджеру отдела продаж!')
            else:
                return True, contacts_list[0]
        elif contact.status_code == 204:
            return False, 'Контакт не найден'
        else:
            logger.error('Нет авторизации в AMO_API')
            return False, 'Произошла ошибка на сервере!'

    @staticmethod
    def _get_customer_id_from_contact(contact_data: dict) -> int | None:
        embedded_data = contact_data.get('_embedded', {})

        customers = embedded_data.get('customers', [])
        if customers:
            return customers[0].get('id')

        customer = embedded_data.get('customer')
        if isinstance(customer, dict):
            return customer.get('id')
        if isinstance(customer, list) and customer:
            return customer[0].get('id')

        return None

    def get_contacts_with_customer(self, limit: int = 250) -> list[AmoContact]:
        url = '/api/v4/contacts'
        page = 1
        all_contacts: list[AmoContact] = []
        attestate_field_id = 1096322

        while True:
            logger.info(f'Запрос контактов, страница: {page}')
            query = f'with=customers&limit={limit}&page={page}'
            response = self._base_request(endpoint=url, type='get_param', parameters=query)

            if response.status_code == 204:
                break

            if response.status_code != 200:
                logger.error(
                    f'Не удалось получить контакты: status_code={response.status_code}, page={page}, body={response.text}'
                )
                break

            page_items = response.json().get('_embedded', {}).get('contacts', [])
            if not page_items:
                break

            for contact in page_items:
                all_contacts.append(
                    AmoContact(
                        contact_id=contact.get('id'),
                        customer_id=self._get_customer_id_from_contact(contact),
                        attestate_at=self._get_custom_field_value(contact, attestate_field_id)
                    )
                )

            if len(page_items) < limit:
                break

            page += 1
            # Ограничение API: не более 3 запросов в секунду.
            time.sleep(0.2)

        return all_contacts


    def add_new_task(self, contact_id, descr, url_materials, time):
        url = '/api/v4/tasks'
        data = [
            {
            'text': f'Обращение по ошибке чат-бота:\n{descr} {url_materials}',
            'complete_till': int(time),
            'entity_id': contact_id,
            "entity_type": "contacts",
            'responsible_user_id': 6390936
        },
            {
            'text': f'Обращение по ошибке чат-бота:\n{descr} {url_materials}',
            'complete_till': int(time),
            'entity_id': contact_id,
            "entity_type": "contacts",
            'responsible_user_id': 10353813
            }
        ]
        response = self._base_request(type='post', endpoint=url, data=data)
        return response


    @staticmethod
    def _get_main_contact_id(lead_data: dict) -> int | None:
        contacts = lead_data.get('_embedded', {}).get('contacts', [])
        if not contacts:
            return None

        for contact in contacts:
            if contact.get('is_main'):
                return contact.get('id')

        return contacts[0].get('id')

    @staticmethod
    def _get_custom_field_value(lead_data: dict, field_id: int):
        custom_fields = lead_data.get('custom_fields_values', [])
        if custom_fields is not None:
            for field in custom_fields:
                if field.get('field_id') == field_id:
                    values = field.get('values', [])
                    if values:
                        return values[0].get('value')
        return None

    @staticmethod
    def _convert_unix_to_sheets_datetime(timestamp_value):
        if timestamp_value in (None, ''):
            return None

        try:
            unix_timestamp = float(timestamp_value)
        except (TypeError, ValueError):
            return timestamp_value

        # Некоторые поля могут приходить в миллисекундах.
        if unix_timestamp > 10_000_000_000:
            unix_timestamp /= 1000

        try:
            dt_value = datetime.fromtimestamp(unix_timestamp)
        except (OverflowError, OSError, ValueError):
            return timestamp_value

        return dt_value.strftime('%d-%m-%Y %H:%M:%S')

    def get_pipeline_1628622_status_142_leads(self, limit: int = 250) -> list[AmoLead]:
        url = '/api/v4/leads'
        page = 1
        all_leads: list[AmoLead] = []
        shipment_field_id = 935651


        while True:
            logger.info(f'Запрос сделок, страница: {page}')
            query = (
                f'filter[pipeline_id][]=1628622&'
                f'filter[statuses][0][pipeline_id]=1628622&'
                f'filter[statuses][0][status_id]=142&'
                f'with=contacts&'
                f'limit={limit}&'
                f'page={page}'
            )
            response = self._base_request(endpoint=url, type='get_param', parameters=query)

            if response.status_code == 204:
                break

            if response.status_code != 200:
                logger.error(
                    f'Не удалось получить сделки: status_code={response.status_code}, page={page}, body={response.text}'
                )
                break

            page_items = response.json().get('_embedded', {}).get('leads', [])
            if not page_items:
                break

            for lead in page_items:
                all_leads.append(
                    AmoLead(
                        lead_id=lead.get('id'),
                        lead_price=lead.get('price'),
                        created_at=lead.get('created_at'),
                        close_at=lead.get('closed_at'),
                        contact_id=self._get_main_contact_id(lead),
                        shipment_at=self._get_custom_field_value(lead, shipment_field_id),

                    )
                )

            if len(page_items) < limit:
                break

            page += 1
            # Ограничение API: не более 2 запросов в секунду.
            time.sleep(0.2)

        return all_leads



    def add_catalog_elements_to_lead(self, lead_id, elements: filter):
        url = f'/api/v4/leads/{lead_id}/link'
        data = []
        for element in elements:
            element_id = int(element.get('id'))
            quantity = int(float(element.get('quantity')))
            element_for_record = {
                'to_entity_id': element_id,
                "to_entity_type": "catalog_elements",
                "metadata": {
                    "quantity": quantity,
                    "catalog_id": 1682
                }
            }
            data.append(element_for_record)
        response = self._base_request(type='post', endpoint=url, data=data)
        return response.json()

    def create_new_contact(self, first_name: str, last_name: str, phone: str):
        url = '/api/v4/contacts'
        data = [{
            'first_name': first_name,
            'last_name': last_name,
            'responsible_user_id': 11047749,
            'custom_fields_values': [
                {"field_id": 671750,
                 "values": [
                     {'enum_code': 'WORK',
                      "value": str(phone)
                      },]
                 }
            ],
        }]
        response = self._base_request(type='post', endpoint=url, data=data)
        contact_id = response.json().get('_embedded').get('contacts')[0].get('id')
        return int(contact_id)

    def send_lead_to_amo(self, contact_id: int, order_id: str):
        url = f'/api/v4/leads'
        data = [{
            'name': 'Заказ с маркета',
            'pipeline_id': 25020,
            'created_by': 0,
            'status_id': 17566048,
            'responsible_user_id': 11047749,
            'custom_fields_values': [
                {"field_id": 1101072,  # Поле id маркетплейса
                 "values": [
                     {"value": str(order_id)},
                 ]
                 }
                ],
            '_embedded': {
                'tags': [
                    {
                    'id': 563936
                    }
                ],
                'contacts': [
                    {
                        'id': contact_id
                    }
                ]
            }

        },]
        response = self._base_request(type='post', endpoint=url, data=data)
        return response.json()

    def add_new_note_to_lead(self, lead_id, text, order_id):
        url = f'/api/v4/leads/{lead_id}/notes'
        market_order_url = f'https://partner.market.yandex.ru/order/{order_id}?partnerId=182087723'
        data = [
            {
                'note_type': 'common',
                'params': {
                    'text': text
                }
            }
        ]
        response = self._base_request(type='post', endpoint=url, data=data)
        return response.json()




if __name__ == '__main__':
    from settings import load_config, Config
    config: Config = load_config(path='../.env')

    amo_api = AmoCRMWrapper(
        path=config.amo_config.path_to_env,
        amocrm_subdomain=config.amo_config.amocrm_subdomain,
        amocrm_client_id=config.amo_config.amocrm_client_id,
        amocrm_redirect_url=config.amo_config.amocrm_redirect_url,
        amocrm_client_secret=config.amo_config.amocrm_client_secret,
        amocrm_secret_code=config.amo_config.amocrm_secret_code,
        amocrm_access_token=config.amo_config.amocrm_access_token,
        amocrm_refresh_token=config.amo_config.amocrm_refresh_token
    )
    amo_api.init_oauth2()






