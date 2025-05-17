from fastapi import FastAPI
import json
from aiogram import Bot
from settings.settings import load_config




config = load_config()
bot=Bot(token=config.tg_bot.token)


app = FastAPI()

@app.post('/')
def get_info(message):
    bot.send_message(chat_id=config.admin_chat_id,
                     text=str(message))


