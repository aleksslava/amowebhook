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
class LeadAdd(BaseModel):
    id: int
    status_id: int
    pipeline_id: int

class Leads(BaseModel):
    add: List[LeadAdd]

class Account(BaseModel):
    id: int
    subdomain: str

class RequestBody(BaseModel):
    leads: Leads
    account: Account


@app.post('/')
async def get_info(request_body: RequestBody):
    await bot.send_message(chat_id=config.admin_chat_id,
                           text=f'{request_body.leads.status}')
