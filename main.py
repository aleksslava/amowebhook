import datetime

import requests
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from aiogram import Bot
import logging
from settings.amo_api import AmoCRMWrapper, build_amo_results
from settings.google_sheets import GoogleSheetsIntegration
from settings.settings import load_config
from utils.utils import correct_phone, Order
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
google_sheets = (
    GoogleSheetsIntegration(config.google_sheets_webhook_url)
    if config.google_sheets_webhook_url else None
)


def _analyze_and_send_to_sheets(token: str, request_id: str):
    try:
        if google_sheets is None:
            logger.error('GOOGLE_SHEETS_WEBHOOK_URL is not configured')
            return

        def _format_day_month_year(value) -> str | None:
            if value is None:
                return None

            if isinstance(value, datetime.datetime):
                return value.strftime('%d-%m-%Y')

            if isinstance(value, datetime.date):
                return value.strftime('%d-%m-%Y')

            if isinstance(value, datetime.timedelta):
                sign = '-' if value.total_seconds() < 0 else ''
                total_days = abs(value.days)
                years, remainder_days = divmod(total_days, 365)
                months, days = divmod(remainder_days, 30)
                return f'{sign}{days:02d}-{months:02d}-{years:04d}'

            return str(value)

        leads_list = amo_api.get_pipeline_1628622_status_142_leads()
        contacts_list = amo_api.get_contacts_with_customer()
        amo_results = build_amo_results(leads=leads_list, contacts=contacts_list)

        payload = [
            {
                'lead_id': amo_result.lead_obj.lead_id,
                'lead_price': amo_result.lead_obj.lead_price,
                'created_at': amo_result.lead_obj.created_at,
                'close_at': amo_result.lead_obj.close_at,
                'shipment_at': amo_result.lead_obj.shipment_at,
                'attestate_at': amo_result.contact_obj.attestate_at,
                'contact_id': amo_result.lead_obj.contact_id,
                'customer_id': amo_result.contact_obj.customer_id,
                'time_from_attestate': _format_day_month_year(amo_result.contact_obj.time_from_attestate),
                'last_buy': _format_day_month_year(amo_result.lead_obj.last_buy),
                'clean_price': amo_result.lead_obj.clean_price,
            }
            for amo_result in amo_results
        ]
        logger.info(f'Значение случайной строки {payload[15]}')

        response = google_sheets.send_json(payload=payload, token=token, request_id=request_id)
        logger.info(
            f'Analyze request finished: request_id={request_id}, '
            f'payload_count={len(payload)}, sheets_status={response.status_code}'
        )
    except Exception as error:
        logger.exception(f'Analyze background task failed: request_id={request_id}, error={error}')


@app.get('/analyze')
async def test(background_tasks: BackgroundTasks, token: str, request_id: str):
    if google_sheets is None:
        raise HTTPException(status_code=500, detail='GOOGLE_SHEETS_WEBHOOK_URL is not configured')
    if config.google_sheets_token is None:
        raise HTTPException(status_code=500, detail='GOOGLE_SHEETS_TOKEN is not configured')
    if token != config.google_sheets_token:
        raise HTTPException(status_code=401, detail='Invalid token')

    background_tasks.add_task(_analyze_and_send_to_sheets, token, request_id)
    return {
        'status': 'accepted',
        'request_id': request_id
    }



@app.post('/sheets')
async def new_column_in_sheet(req: Request):
    response = await req.json()
    time_add = response.get('timestamp')
    time = datetime.datetime.strptime(time_add, "%d.%m.%Y %H:%M:%S")
    two_hours = datetime.timedelta(hours=2)
    time = time + two_hours
    time = time.timestamp()

    phone = correct_phone(response.get('phone'))

    contact = amo_api.get_contact_by_phone(phone)
    fullname = response.get('fullName')
    description = response.get('description')
    materials = response.get('materialsLink', '')
    if contact[0]:
        contact_id = contact[1].get('id')
    else:
        await bot.send_message(chat_id=config.admin_chat_id, text=f'Не получилось найти id контакта.\n'
                                                                  f'Номер телефона {phone}\n'
                                                                  f'ФИО: {fullname}')
        raise ValueError('Не получилось найти id контакта.')



    response = amo_api.add_new_task(contact_id=contact_id, descr=description, url_materials=materials, time=time)
    logger.info(response.status_code)


@app.post('/sheets/marketplace')
async def new_column_in_sheet(req: Request):
    response = await req.json()
    lead_id = int(response.get('data').get('lead_id'))
    items = response.get('data').get('items')
    items = filter(lambda x: float(x.get('quantity')) >= 1, items)
    response_put_to_lead = amo_api.add_catalog_elements_to_lead(lead_id=lead_id, elements=items)
    logger.info(response_put_to_lead)


@app.post('/market/new_order/notification')
async def new_order_from_yandex(req:Request):
    response = await req.json()
    try:
        response = await req.json()
        order_id = response.get('orderId')
        await bot.send_message(chat_id=config.admin_chat_id,
                               text=str(response))
        url_order = f'https://api.partner.market.yandex.ru/v2/campaigns/{config.magazne_id}/orders/{order_id}'
        order_data = requests.get(url=url_order, headers={'Api-Key': config.yandex_api_key}).json()
        order_data = Order(order_data=order_data)
        order_data.get_buyer()

        url_bayer = f'https://api.partner.market.yandex.ru/v2/campaigns/{config.magazne_id}/orders/{order_id}/buyer'

        buyer_info = requests.get(url=url_bayer, headers={'Api-Key': config.yandex_api_key}).json().get('result')
        buyer_phone = buyer_info.get('phone')

        contact_id = amo_api.create_new_contact(first_name=order_data.buyer_firstname,
                                                last_name=order_data.buyer_lastname,
                                                phone=buyer_phone)
        logger.info(f'Создан контакт id {contact_id}')
        new_lead = amo_api.send_lead_to_amo(contact_id=contact_id, order_id=order_id)
        lead_id = new_lead.get('_embedded').get('leads')[0].get('id')
        logger.info(f"Создана новая сделка: {lead_id}")

        amo_api.add_new_note_to_lead(lead_id=lead_id, text=order_data.order_items + order_data.address, order_id=order_id)
    except BaseException as error:
        logger.error(error)
        logger.error(f'Не получилось обработать вебхук {response}')
    finally:
        return {
            "version": "1.0.0",
            "name": "Amowebhooks",
            "time": '2025-11-20T11:09:26.246Z'
            }







