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
    customer_id = int(data.get('catalogs[add][0][custom_fields][0][values][0][value]'))
    lead_bonus = int(data.get('catalogs[add][0][custom_fields][2][values][0][value]'))
    lead_price = int(data.get('catalogs[add][0][custom_fields][1][values][0][value]'))
    list_id = int(data.get('catalogs[add][0][id]'))
    purified_price = lead_price - lead_bonus
    logger.error(lead_bonus)
    logger.error(dict(data))

    # customer_obj = amo_api.get_customer_by_id(customer_id)
    # if customer_obj[0]:
    #     last_full_price = get_full_price_customer(customer_obj[1])
    #     new_price = purified_price + int(last_full_price)
    #
    #     amo_api.put_full_price_to_customer(id_customer=customer_id,
    #                                         new_price=new_price)
    #
    #     await bot.send_message(chat_id=config.admin_chat_id,
    #                            text=f'В покупателя id '
    #                                 f'<a href="https://hite.amocrm.ru/customers/detail/{customer_id}">{customer_id}</a>.'
    #                                 f', добавлен чистый выкуп {purified_price} руб.\n'
    #                                 f'Запись в логе бонусов id <a href="https://hite.amocrm.ru/catalogs/2244/detail/{list_id}">{list_id}</a>.')
    # else:
    #     await bot.send_message(chat_id=config.admin_chat_id,
    #                            text=f'Произошла ошибка при добавлении чистого выкупа в покупателя id <a href="https://hite.amocrm.ru/customers/detail/{customer_id}">{customer_id}</a>.\n'
    #                                 f'Запись в логе бонусов id <a href="https://hite.amocrm.ru/catalogs/2244/detail/{list_id}">{list_id}</a>.')
    #
    #
    #



