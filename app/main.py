from fastapi import FastAPI, Body, Query, Form
import json
from aiogram import Bot
from pydantic import BaseModel

from settings.settings import load_config
from typing import Annotated, List

config = load_config()
bot = Bot(token=config.tg_bot.token)

app = FastAPI()

# Определение моделей данных
class Status(BaseModel):
    id: int
    old_pipeline_id: int
    pipeline_id: int
    old_status_id: int
    status_id: int

class Leads(BaseModel):
    status: List[Status]

class RequestBody(BaseModel):
    leads: Leads


@app.post('/')
async def get_info(request_body: Annotated[str, Form()]):
    await bot.send_message(chat_id=config.admin_chat_id,
                           text=f'{request_body}')
