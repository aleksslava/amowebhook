from fastapi import FastAPI, Body
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
    id: str
    old_pipeline_id: str
    pipeline_id: str
    old_status_id: str
    status_id: str

class Leads(BaseModel):
    status: List[Status]

class RequestBody(BaseModel):
    leads: Leads


@app.post('/')
async def get_info(request_body: RequestBody):
    await bot.send_message(chat_id=config.admin_chat_id,
                           text=f'{request_body.leads.status}')
