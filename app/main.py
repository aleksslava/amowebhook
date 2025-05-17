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
    id: Annotated[str, Form()]
    old_pipeline_id: Annotated[str, Form()]
    pipeline_id: Annotated[str, Form()]
    old_status_id: Annotated[str, Form()]
    status_id: Annotated[str, Form()]

class Leads(BaseModel):
    status: Annotated[List[Status], Form()]

class RequestBody(BaseModel):
    leads: Annotated[Leads, Form()]


@app.post('/')
async def get_info(
    leads_add_0_id: int = Form(...),
    leads_add_0_status_id: int = Form(...),
    leads_add_0_pipeline_id: int = Form(...),
    account_id: int = Form(...),
    account_subdomain: str = Form(...)
):
    await bot.send_message(chat_id=config.admin_chat_id,
                           text=f'{leads_add_0_id}')
