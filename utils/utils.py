import datetime
import logging

from pydantic import json

logger = logging.getLogger(__name__)


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



def convert_data(timestamp: int) -> str :
    if timestamp is None or timestamp == 0:
        return ''
    else:
        current_time = datetime.datetime.fromtimestamp(timestamp)
        current_time = current_time.strftime('%d-%m-%Y %H:%M:%S')
        return current_time

def conver_timestamp_to_days(timestamp: int) -> str:
    if timestamp is None or timestamp == 0:
        return ''
    else:
        current_time = int(timestamp / 86400)
        return str(current_time)


def get_catalog_elements_from_lead(response: dict) -> dict[int, int | float]:
    result: dict[int, int | float] = {}

    catalog_elements = (
        (response or {})
        .get('_embedded', {})
        .get('catalog_elements', [])
    )

    for element in catalog_elements:
        element_id = element.get('id')
        quantity = (element.get('metadata') or {}).get('quantity')

        if element_id is None or quantity is None:
            continue

        try:
            product_id = int(element_id)
            quantity_value = float(quantity)
            result[product_id] = int(quantity_value) if quantity_value.is_integer() else quantity_value
        except (TypeError, ValueError) as error:
            logger.warning(f'Не удалось распарсить catalog_element: {element}, error={error}')

    return result


def get_items_to_kp(
        response: dict,
        catalog_elements: dict[int, int | float],
        discount: int
) -> list[dict[str, int | float | str]]:
    items: list[dict[str, int | float | str]] = []
    elements = (response or {}).get('_embedded', {}).get('elements', [])

    for element in elements:
        name = element.get('name')
        if not name:
            continue

        element_id = element.get('id')
        try:
            element_id = int(element_id)
        except (TypeError, ValueError):
            logger.warning(f'Некорректный id элемента каталога: {element}')
            continue

        price_value = None
        custom_fields = element.get('custom_fields_values') or []
        for field in custom_fields:
            field_code = field.get('field_code')
            field_name = field.get('field_name')
            if field_code == 'PRICE' or field_name == 'Цена':
                values = field.get('values') or []
                if values:
                    price_value = values[0].get('value')
                break

        price: int | float = 0
        if price_value not in (None, ''):
            try:
                raw_price = float(str(price_value).replace(',', '.'))
                price = int(raw_price) if raw_price.is_integer() else raw_price
            except (TypeError, ValueError) as error:
                logger.warning(f'Не удалось распарсить цену элемента: {element}, error={error}')

        quantity_value = catalog_elements.get(element_id, 0)
        try:
            quantity_number = float(quantity_value)
            quantity = int(quantity_number) if quantity_number.is_integer() else quantity_number
        except (TypeError, ValueError):
            quantity = 0

        total_value = float(price) * float(quantity) if quantity else 0
        total = int(total_value) if float(total_value).is_integer() else total_value

        items.append({
            'name': name,
            'price': price,
            'discount': discount,
            'quantity': quantity,
            'total': total,
        })

    return items

