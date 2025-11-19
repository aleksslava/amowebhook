import logging
from typing import List, Dict
from pydantic import BaseModel
logger = logging.getLogger(__name__)

def get_lead_bonus(lst: List):
    if lst is None:
        return 0
    bonus = [res for res in lst if res['field_id'] == 1105034]
    if not bonus:
        return 0
    bonus = bonus[0].get('values')[0].get('value')
    return bonus

def get_lead_bonus_off(lst: List):
    if lst is None:
        return 0
    bonus = [res for res in lst if res['field_id'] == 1105036]
    if not bonus:
        return 0
    bonus = bonus[0].get('values')[0].get('value')
    return abs(int(bonus))

def get_main_contact(lst: List):
    main_contact = [res for res in lst if res['is_main']]

    if main_contact is None:
        return False

    main_contact_id = main_contact[0].get('id')
    return main_contact_id

def get_customer_id(dct: Dict):
    customers = dct.get('_embedded').get('customers')

    customer = customers[0]
    return customer.get('id')

def get_full_price_customer(dct: Dict):
    custom_fields_list = dct.get('custom_fields_values', [])

    full_price = [res for res in custom_fields_list if res['field_id'] == 1105022]

    if not full_price:
        return 0

    res = full_price[0].get('values')[0].get('value')

    return res

def get_full_bonus_customer(dct: Dict):
    custom_fields_list = dct.get('custom_fields_values', [])

    full_bonus = [res for res in custom_fields_list if res['field_id'] == 971580]

    if not full_bonus:
        return 0

    res = full_bonus[0].get('values')[0].get('value')

    return res

def get_lead_total(record):
    field_total_id = 1105084
    field_type_id = 1105600
    fields_values = record.get('custom_fields_values')
    value = 0
    record_type = ''
    for field in fields_values:
        if field.get('field_id') == field_total_id:
            value = field.get('values')[0].get('value', 0)
            value = int(float(value)//1)
        if field.get('field_id') == field_type_id:
            record_type = field.get('values')[0].get('value')
    if record_type == 'Возврат':
        return -int(value)
    else:
        return int(value)


def get_bonus_total(record):
    field_total_id = 1105086
    field_type_id = 1105600
    fields_values = record.get('custom_fields_values')
    value = 0
    record_type = ''
    for field in fields_values:
        if field.get('field_id') == field_total_id:
            value = field.get('values')[0].get('value', 0)
            value = int(float(value)//1)
        if field.get('field_id') == field_type_id:
            record_type = field.get('values')[0].get('value')
    if record_type == 'Корректировка':
        return 0
    else:
        return value

def correct_phone(phone_str: str):
    new_str = ('')
    for char in phone_str:
        if char.isnumeric():
            new_str += char
    if new_str.startswith('7') or new_str.startswith('8'):
        new_str = new_str[1:]
    return new_str



class Order:
    def __init__(self, order_data: dict):
        self.order_data = order_data.get('order')

    def get_buyer(self):
        self.buyer_id = self.order_data.get('buyer').get('id')
        self.buyer_firstname = self.order_data.get('buyer').get('firstName', '')
        self.buyer_lastname = self.order_data.get('buyer').get('lastName', '')
        self.buyer_email = self.order_data.get('buyer').get('email', '')
        self.order_items = self.get_items()
        self.address = self.get_delivery_parameters()

    def get_items(self):
        items_list = self.order_data.get('items')
        items_res = 'Состав заказа:\n\n'
        total = self.order_data.get('itemsTotal', 0)
        for item in items_list:
            item_price = int(item.get('buyerPrice', 0))
            count = int(item.get('count', 0))
            item_name = item.get('offerName')
            items_res += f'{item_name}: {count} шт. по  {item_price} руб. = {item_price*count} руб.\n'
        items_res += f'\nИтого: {total}\n'
        return items_res

    def get_delivery_parameters(self):
        raw_adress = self.order_data.get('delivery').get('address')
        if raw_adress is None:
            raise ValueError
        city = raw_adress.get('city')
        country = raw_adress.get('country')
        street = raw_adress.get('street')
        house = raw_adress.get('house')
        return f'Адрес:\nСтрана: {country}, Город: {city}, улица: {street}, дом: {house}'


