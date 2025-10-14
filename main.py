from pprint import pprint

from fastapi import FastAPI, Request
from aiogram import Bot
import logging
from settings.amo_api import AmoCRMWrapper
from settings.settings import load_config
from utils.utils import get_lead_total, get_bonus_total
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
    logger.info(f'Получен webhook: {dict(data)}')
    list_id = int(data.get('catalogs[add][0][id]', default=0))
    customer_id = int(data.get('catalogs[add][0][custom_fields][1][values][0][value]'))

    # Запрашиваем список записей покупателя
    response = amo_api.get_catalog_elements_by_partnerid(partner_id=customer_id)

    # считаем суммарное значение отгрузок\возвратов
    elements_list = response.get('_embedded').get('elements')
    elements_total = list(map(get_lead_total, elements_list))
    sum_total = sum(elements_total)
    logger.info(f'Список значений чистого выкупа покупателя: {elements_total}, сумма: {sum_total}')

    # считаем суммарное значение бонусов
    bonus_total = list(map(get_bonus_total, elements_list))
    sum_bonus = sum(bonus_total)
    sum_response = sum_total - sum_bonus
    logger.info(f'Список значений бонусов покупателя: {bonus_total}, сумма: {sum_bonus}')
    logger.info(f'Итоговая сумма чистого выкупа: {sum_response}')
    # записываем новое значение в сумму чистого выкупа
    response = amo_api.put_full_price_to_customer(id_customer=customer_id, new_price=sum_response)
    if response.status_code == 200:
        await bot.send_message(chat_id=config.admin_chat_id,
                               text=f'Новая запись в покупателя id '
                                    f'<a href="https://hite.amocrm.ru/customers/detail/{customer_id}">{customer_id}</a>.'
                                    f'Список значений отгрузок\вовратов: {elements_total}.\n'
                                    f'Список начислений\списаний бонусов: {bonus_total}\n'
                                    f'Итоговый чистый выкуп: {sum_response}'
                                    f'Запись в логе бонусов id <a href="https://hite.amocrm.ru/catalogs/2244/detail/{list_id}">{list_id}</a>.',
                               parse_mode=ParseMode.HTML)
    else:
        await bot.send_message(chat_id=config.admin_chat_id,
                               text=f'Не удалось просчитать чистый выкуп в покупателе <a href="https://hite.amocrm.ru/customers/detail/{customer_id}">{customer_id}</a>.\n'
                                    f'Запись в логе бонусов id <a href="https://hite.amocrm.ru/catalogs/2244/detail/{list_id}">{list_id}</a>.\n',
                               parse_mode=ParseMode.HTML)


@app.post('/sheets')
async def new_column_in_sheet(req: dict):
    # phone = str(req.get('Телефон')).replace('+', '')
    # description = str(req.get('Ошибка'))
    # contact = amo_api.get_contact_by_phone(phone_number=int(phone))[1]
    # contact_id = contact.get('id')


    logger.info(f'{req}')