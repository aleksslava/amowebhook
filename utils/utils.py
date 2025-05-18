from typing import List


def get_lead_bonus(lst: List):
    if lst is None:
        return 0
    bonus = [res for res in lst if res['field_id'] == '1105034']

    if bonus is None:
        return 0
    bonus = bonus[0].get('values')[0].get('value')
    return bonus

def get_main_contact(lst: List):
    main_contact = [res for res in lst if res['is_main']]

    if main_contact is None:
        return False

    main_contact_id = main_contact[0].get('id')
    return main_contact_id