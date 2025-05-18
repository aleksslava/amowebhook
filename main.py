from fastapi import FastAPI, Request
from aiogram import Bot
import logging
from settings.amo_api import AmoCRMWrapper
from settings.settings import load_config
from utils.utils import get_lead_bonus, get_main_contact

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
    # await bot.send_message(chat_id=config.admin_chat_id,
    #                        text=f'{data}, {id}')

    lead = amo_api.get_lead_with_contacts(lead_id=lead_id)
    if lead[0]:
        lead = lead[1]

        lead_price = lead.get('price')
        custom_fields = lead.get('custom_fields_values')
        lead_bonus = get_lead_bonus(custom_fields)

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
                await bot.send_message(chat_id=config.admin_chat_id,
                                       text=contact[1])

            else:
                await bot.send_message(chat_id=config.admin_chat_id,
                                       text=f'Произошла ошибка!\n'
                                            f'Сделка id {lead_id}\n'
                                            f'Контакт id {main_contact_id}\n'
                                            f'Текст ошибки: {contact[1]}')

    else:
        await bot.send_message(chat_id=config.admin_chat_id,
                               text=f"Произошла ошибка, бот не нашёл сделку №{lead_id}")

    await bot.send_message(chat_id=config.admin_chat_id,
                           text=str(lead))

