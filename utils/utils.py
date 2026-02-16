import logging
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


