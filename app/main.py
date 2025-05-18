from fastapi import FastAPI, Request
from aiogram import Bot
from settings.settings import load_config
from settings.amo_api import AmoCRMWrapper


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

    await bot.send_message(chat_id=config.admin_chat_id,
                           text=str(lead))

