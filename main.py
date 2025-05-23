from fastapi import FastAPI, Request
from aiogram import Bot
import logging
from settings.amo_api import AmoCRMWrapper
from settings.settings import load_config
from utils.utils import (get_lead_bonus, get_main_contact, get_customer_id, get_full_price_customer,
                         get_full_bonus_customer, get_lead_bonus_off)
from aiogram.enums.parse_mode import ParseMode

logger = logging.getLogger(__name__)
logging.basicConfig(
        level=logging.INFO,
        format='%(filename)s:%(lineno)d #%(levelname)-8s '
               '[%(asctime)s] - %(name)s - %(message)s')

config = load_config()
bot = Bot(token=config.tg_bot.token)

app = FastAPI()

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



@app.post('/')
async def get_info(req: Request):
    data = await req.form()
    lead_id = data.get('leads[add][0][id]')

    lead = amo_api.get_lead_with_contacts(lead_id=lead_id)
    if lead[0]:
        lead = lead[1]

        lead_price = lead.get('price')
        custom_fields = lead.get('custom_fields_values')
        lead_bonus = get_lead_bonus(custom_fields)
        lead_bonus_off = get_lead_bonus_off(custom_fields)

        contacts = lead.get('_embedded').get('contacts')
        main_contact_id = get_main_contact(contacts)

        if not main_contact_id:
            await bot.send_message(chat_id=config.admin_chat_id,
                                   text=f"Произошла ошибка,\n"
                                        f"Сделка id {lead_id}\n"
                                        f"Не удалось извлечь id главного контакта сделки.")

        else:
            contact = amo_api.get_contact_by_id(main_contact_id)
            if contact[0]:
                contact = contact[1]
                try:
                    customer_id = get_customer_id(contact)
                except:
                    await bot.send_message(chat_id=config.admin_chat_id,
                                           text=f'Произошла ошибка при попытке получить id покупателя из контакта.\n'
                                                f'id сделки: {lead_id}\n'
                                                f'id контакта: {main_contact_id}')
                    raise ValueError
                customer_obj = amo_api.get_customer_by_id(customer_id)
                if customer_obj[0]:
                    last_full_price = get_full_price_customer(customer_obj[1])
                    last_full_bonus = get_full_bonus_customer(customer_obj[1])
                    new_full_price = int(last_full_price) + int(lead_price) - int(lead_bonus)
                    new_full_bonus = int(last_full_bonus) + int(lead_bonus) - int(lead_bonus_off)
                    amo_api.put_full_price_to_customer(id_customer=customer_id,
                                                       new_price=new_full_price,
                                                       new_bonus=new_full_bonus)
                    await bot.send_message(chat_id=config.admin_chat_id,
                                           text=f'Успешная запись в покупателя id'
                                                f'<a href="https://hite.amocrm.ru/customers/detail/{customer_id}">{customer_id}</a>.\n'
                                                f'Сделка id '
                                                f'<a href="https://hite.amocrm.ru/leads/detail/{lead_id}">{lead_id}</a> / '
                                                f'Контакт id '
                                                f'<a href="https://hite.amocrm.ru/contacts/detail/{main_contact_id}">{main_contact_id}</a>\n'
                                                f'Сумма сделки - {lead_price}, бонусов {"начислено" if int(lead_bonus) - int(lead_bonus_off) >= 0 else "списано"} {int(lead_bonus)-int(lead_bonus_off)}\n\n'
                                                f'Прошлое значение чистого выкупа - {last_full_price}\n'
                                                f'Прошлое значение бонусов на балансе - {last_full_bonus}\n\n'
                                                f'Добавлено в чистый выкуп - {int(lead_price) - int(lead_bonus)}\n'
                                                f'Добавлено в бонусы на балансе - {lead_bonus}\n\n'
                                                f'Новая сумма чистого выкупа - {new_full_price}\n'
                                                f'Новая сумма бонусов на балансе - {new_full_bonus}',
                                           parse_mode=ParseMode.HTML)

                else:
                    await bot.send_message(chat_id=config.admin_chat_id,
                                           text=f'Ошибка при попытке получить сущность покупателя.\n'
                                                f'Сделка id {lead_id}\n'
                                                f'Контакт id {main_contact_id}\n'
                                                f'Покупатель id {customer_id}')


            else:
                await bot.send_message(chat_id=config.admin_chat_id,
                                       text=f'Произошла ошибка!\n'
                                            f'Сделка id {lead_id}\n'
                                            f'Контакт id {main_contact_id}\n'
                                            f'Текст ошибки: {contact[1]}')

    else:
        await bot.send_message(chat_id=config.admin_chat_id,
                               text=f"Произошла ошибка, бот не нашёл сделку №{lead_id}")



