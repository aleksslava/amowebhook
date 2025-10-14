import logging
from typing import List, Dict
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

