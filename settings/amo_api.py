import pprint

import dotenv
import jwt
import requests
from datetime import datetime
import logging

from pydantic import json
from requests.exceptions import JSONDecodeError

logger = logging.getLogger(__name__)



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

    def get_customer_by_phone(self, phone_number) -> tuple:
        contact = self.get_contact_by_phone(phone_number, with_customer=True)
        if contact[0]:  # Проверка, что ответ от сервера получен
            contact = contact[1]
            customer_list = contact['_embedded']['customers']
            if len(customer_list) > 1:
                return False, 'К номеру телефона привязано более одного партнёра'
            elif not customer_list:
                return False, 'К номеру телефона не привязано ни одного партнёра'
            customer_id = customer_list[0]['id']
            url = f'/api/v4/customers/{customer_id}'
            customer = self._base_request(endpoint=url, type='get').json()

            return True, customer
        else:
            return contact

    def get_customer_by_id(self, customer_id, with_contacts=False) -> tuple:
        url = f'/api/v4/customers/{customer_id}'
        try:
            if with_contacts:
                query = str(f'with=contacts')
                customer = self._base_request(endpoint=url, type='get_param', parameters=query)
            else:
                customer = self._base_request(endpoint=url, type='get')
        except Exception as error:
            return False, "Произошла ошибка на сервере"
        if customer.status_code == 200:
            return True, customer.json()
        elif customer.status_code == 204:
            return False, 'Партнёр не найден!'
        else:
            logger.error('Нет авторизации в AMO_API')
            return False, 'Произошла ошибка на сервере!'

    def get_customer_by_tg_id(self, tg_id: int) -> dict:  # Нужно убрать все id полей амо в конфиг
        url = '/api/v4/customers'
        field_id = '1104992'
        query = str(f'filter[custom_fields_values][{field_id}][]={tg_id}')
        response = self._base_request(endpoint=url, type='get_param', parameters=query)

        if response.status_code == 200:
            customer_list = response.json()['_embedded']['customers']
            if len(customer_list) > 1:
                return {'status_code': False,
                        'tg_id_in_db': False,
                        'response': 'Найдено более одного номера tg_id в базе данных\n'
                                    'Обратитесь к Вашему менеджеру'
                        }
            return {'status_code': True,
                    'tg_id_in_db': True,
                    'response': customer_list[0]
                    }

        elif response.status_code == 204:
            return {'status_code': True,
                    'tg_id_in_db': False,
                    'response': 'Телеграмм id не найден в базе данных'
                    }

        else:
            return {'status_code': False,
                    'tg_id_in_db': False,
                    'response': 'Произошла ошибка на сервере'
                    }

    def put_tg_id_to_customer(self, id_customer, tg_id):
        url = f'/api/v4/customers/{id_customer}'
        data = {"custom_fields_values": [
            {"field_id": 1104992,
             "values": [
                 {"value": f"{tg_id}"},
                 ]
             }]}
        response = self._base_request(type='patch', endpoint=url, data=data)
        logger.info(f'Запись ID_telegram: {tg_id} в карту партнёра: {id_customer}\n'
                    f'Статус операции: {response.status_code}')

    def get_catalog_elements_by_partnerid(self, partner_id):
        catalog_id = 2244
        url = f'/api/v4/catalogs/{catalog_id}/elements'
        limit = 250
        page = 1
        filter = str(
            f'filter[custom_fields][1105082][from]={partner_id}&filter[custom_fields][1105082][to]={partner_id}')
        response = self._base_request(type='get_param', endpoint=url, parameters=filter)
        logger.info(f'Статус код запроса записей покупателя: {response.status_code}')
        return response.json()

    def get_customers_list_if_tg(self):
        url = f'/api/v4/customers/'
        limit = 250
        page = 1
        filter = str(
            f'filter[custom_fields][5B1104992][from]=1')
        response = self._base_request(type='get_param', endpoint=url, parameters=filter)
        logger.info(f'Статус код запроса записей покупателя: {response.status_code}')
        return response.json()

    def get_contact_by_id(self, contact_id) -> tuple:
        url = f'/api/v4/contacts/{contact_id}'
        query = 'with=customers'
        contact = self._base_request(type='get_param', endpoint=url, parameters=query)
        if contact.status_code == 200:
            return True, contact.json()

        elif contact.status_code == 204:
            return False, f'Контакт {contact_id} не найден'
        else:
            logger.error('Нет авторизации в AMO_API')
            return False, 'Произошла ошибка на сервере!'

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


    def get_responsible_user_by_id(self, manager_id: int):
        url = f'/api/v4/users/{manager_id}'

        responsible_manager = self._base_request(endpoint=url, type='get')
        if responsible_manager.status_code == 200:
            return responsible_manager.json()
        else:
            raise JSONDecodeError

    def get_lead_with_contacts(self, lead_id):
        url = f'/api/v4/leads/{lead_id}'
        query = 'with=contacts'

        lead = self._base_request(endpoint=url, type="get_param", parameters=query)
        if lead.status_code == 200:
            return True, lead.json()


        elif lead.status_code == 204:
            return False, f'Сделка {lead_id} не найдена'
        else:
            logger.error('Нет авторизации в AMO_API')
            return False, 'Произошла ошибка на сервере!'

    def put_full_price_to_customer(self, id_customer, new_price):
        url = f'/api/v4/customers/{id_customer}'
        data = {"custom_fields_values": [
            {"field_id": 1105022,
             "values": [
                 {"value": f"{new_price}"},
                 ]
             }]}
        response = self._base_request(type='patch', endpoint=url, data=data)
        logger.info(f'Статус записи нового чистого выкупа в покупателя: {response.status_code}')
        return response

    def add_catalog_elements_to_lead(self, lead_id, elements: list[dict,]):
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
                {"field_id": 671750,  # Поле проект
                 "values": [
                     {"value": phone},
                 ]
                 }
            ],
        }]
        response = self._base_request(type='post', endpoint=url, data=data)
        contact_id = response.json().get('_embedded').get('contacts')[0].get('id')
        return contact_id

    def send_lead_to_amo(self, contact_id: int, custom_fields_data: list):
        url = f'/api/v4/leads'
        data = [{
            'name': 'Заказ с чат_бота',
            'pipeline_id': 25020,
            'created_by': 0,
            'status_id': 17566048,
            'responsible_user_id': 453498,
            'custom_fields_values': custom_fields_data,
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






