from fastapi import FastAPI, Body
import json
from aiogram import Bot
from settings.settings import load_config
from typing import Annotated

config = load_config()
bot = Bot(token=config.tg_bot.token)

app = FastAPI()


@app.post('/')
async def get_info(message: Annotated[str, Body()]):
    await bot.send_message(chat_id=config.admin_chat_id,
                           text=str(message))
