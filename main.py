from pprint import pprint

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
    # Получаем данные из webhook
    data = await req.form()
    logger.info(dict(data))

    customer_id = int(data.get('catalogs[add][0][custom_fields][1][values][0][value]'))
    lead_bonus = int(data.get('catalogs[add][0][custom_fields][3][values][0][value]', default=0))
    lead_price = float(data.get('catalogs[add][0][custom_fields][2][values][0][value]', default=0))
    lead_price = int(lead_price // 1)
    list_id = int(data.get('catalogs[add][0][id]', default=0))
    type_document = data.get('catalogs[add][0][custom_fields][5][values][0][value]', default='Отгрузка')
    logger.info(
        f'customer_id = {customer_id}, lead_bonus = {lead_bonus}, lead_price = {lead_price}, list_id = {list_id}'
        f'type_document = {type_document}'
    )
    try:
        # Получаем данные покупателя из АМО
        customer_obj = amo_api.get_customer_by_id(customer_id)
        last_full_price = get_full_price_customer(customer_obj[1])

        if type_document == 'Отгрузка': # Отгрузка
            if lead_bonus < 0:
                purified_price = lead_price + abs(lead_bonus)
            else:
                purified_price = lead_price - abs(lead_bonus)
            new_price = purified_price + int(last_full_price)
            logger.info(f'new_price = {new_price}')
        elif type_document == 'Возврат':  # возврат
            if lead_bonus < 0:
                purified_price = lead_price - abs(lead_bonus)
            else:
                purified_price = lead_price + abs(lead_bonus)
            new_price = int(last_full_price) - purified_price
            logger.info(f'new_price = {new_price}')
        elif type_document == 'Корректировка':
            purified_price = lead_price
            new_price = int(last_full_price) + purified_price
            logger.info(f'new_price = {new_price}')
        else:
            raise ValueError(f'Ошибка типа документа. Тип {type_document}')

        # Записываем новое значение чистого выкупа в покупателя
        amo_api.put_full_price_to_customer(id_customer=customer_id,
                                           new_price=new_price)

        # Отправляем уведомление в чат бота
        await bot.send_message(chat_id=config.admin_chat_id,
                               text=f'В покупателя id '
                                    f'<a href="https://hite.amocrm.ru/customers/detail/{customer_id}">{customer_id}</a>.'
                                    f', добавлен чистый выкуп {purified_price} руб.\n'
                                    f'Запись в логе бонусов id <a href="https://hite.amocrm.ru/catalogs/2244/detail/{list_id}">{list_id}</a>.',
                               parse_mode=ParseMode.HTML)
    except BaseException as error:

        await bot.send_message(chat_id=config.admin_chat_id,
                               text=f'Не удалось изменить чистый выкуп в покупателе <a href="https://hite.amocrm.ru/customers/detail/{customer_id}">{customer_id}</a>.\n'
                                    f'Запись в логе бонусов id <a href="https://hite.amocrm.ru/catalogs/2244/detail/{list_id}">{list_id}</a>.\n'
                                    f'Ошибка - {error}',
                               parse_mode=ParseMode.HTML)
