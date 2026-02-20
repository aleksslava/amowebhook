import asyncio
import datetime
import logging
from urllib.parse import unquote_plus, urlencode

import requests
from aiogram import Bot
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from models import Base, EducationVisit
from settings.async_amo_api import AmoCRMWrapperAsync, build_amo_results
from settings.google_sheets import GoogleSheetsIntegration
from settings.settings import load_config
from utils.utils import Order, conver_timestamp_to_days, convert_data, correct_phone

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(filename)s:%(lineno)d #%(levelname)-8s "
           "[%(asctime)s] - %(name)s - %(message)s"
)

config = load_config()
bot = Bot(token=config.tg_bot.token)

app = FastAPI()

db_connect_args = {"check_same_thread": False} if config.database_url.startswith("sqlite") else {}
db_engine = create_engine(config.database_url, connect_args=db_connect_args)
SessionLocal = sessionmaker(bind=db_engine, autocommit=False, autoflush=False)

amo_api = AmoCRMWrapperAsync(
    path=config.amo_config.path_to_env,
    amocrm_subdomain=config.amo_config.amocrm_subdomain,
    amocrm_client_id=config.amo_config.amocrm_client_id,
    amocrm_redirect_url=config.amo_config.amocrm_redirect_url,
    amocrm_client_secret=config.amo_config.amocrm_client_secret,
    amocrm_secret_code=config.amo_config.amocrm_secret_code,
    amocrm_access_token=config.amo_config.amocrm_access_token,
    amocrm_refresh_token=config.amo_config.amocrm_refresh_token,
)

google_sheets = (
    GoogleSheetsIntegration(config.google_sheets_webhook_url)
    if config.google_sheets_webhook_url else None
)


@app.on_event("startup")
async def on_startup() -> None:
    Base.metadata.create_all(bind=db_engine)
    await amo_api.open()
    # Обычно init_oauth2() НЕ вызывают на каждый старт, если токены уже сохранены в .env


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await amo_api.close()


def _cookie_value(cookies: dict[str, str], key: str) -> str | None:
    value = cookies.get(key)
    if value is not None:
        return value
    for cookie_key, cookie_value in cookies.items():
        if cookie_key.strip() == key:
            return cookie_value
    return None


def _normalize_tracking_value(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if cleaned.lower() in {"(none)", "none", "null", "undefined"}:
        return None
    return cleaned


def _parse_sourcebuster_cookie(cookie_value: str | None) -> dict[str, str]:
    parsed: dict[str, str] = {}
    if not cookie_value:
        return parsed

    decoded = unquote_plus(cookie_value).strip()
    for item in decoded.split("|||"):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _get_tracking_value(
        query_params,
        cookies: dict[str, str],
        key: str,
        sbjs_current: dict[str, str],
        sbjs_first: dict[str, str],
        sbjs_key: str,
) -> str | None:
    value = _normalize_tracking_value(query_params.get(key))
    if value is not None:
        return value

    value = _normalize_tracking_value(_cookie_value(cookies, key))
    if value is not None:
        return value

    value = _normalize_tracking_value(sbjs_current.get(sbjs_key))
    if value is not None:
        return value

    return _normalize_tracking_value(sbjs_first.get(sbjs_key))


def _build_payload(amo_results):
    return [
        {
            "lead_id": r.lead_obj.lead_id,
            "lead_price": r.lead_obj.lead_price,
            "created_at": convert_data(r.lead_obj.created_at),
            "close_at": convert_data(r.lead_obj.close_at),
            "shipment_at": convert_data(r.lead_obj.shipment_at),
            "attestate_at": convert_data(r.customer_obj.created_at),
            "contact_id": r.lead_obj.contact_id,
            "customer_id": r.customer_obj.customer_id,
            "clean_price": r.lead_obj.clean_price,
            "last_buy": conver_timestamp_to_days(r.lead_obj.last_buy),
            "time_from_attestate": conver_timestamp_to_days(r.lead_obj.time_from_attestate),
        }
        for r in amo_results
    ]


async def _analyze_and_send_to_sheets(token: str, request_id: str) -> None:
    try:
        if google_sheets is None:
            logger.error("GOOGLE_SHEETS_WEBHOOK_URL is not configured")
            return

        leads_list, customers_list = await asyncio.gather(
            amo_api.get_pipeline_1628622_status_142_leads(),
            amo_api.get_customers_with_contacts(),
        )

        amo_results = build_amo_results(leads=leads_list, customers=customers_list)
        payload = _build_payload(amo_results)

        # Если GoogleSheetsIntegration синхронный (requests) — отправляем в thread, чтобы не блокировать event loop
        response = await asyncio.to_thread(
            google_sheets.send_json,
            payload=payload,
            token=token,
            request_id=request_id,
        )

        logger.info(
            f"Analyze request finished: request_id={request_id}, "
            f"payload_count={len(payload)}, sheets_status={response.status_code}"
        )
    except Exception as error:
        logger.exception(f"Analyze background task failed: request_id={request_id}, error={error}")


@app.get("/analyze")
async def analyze(background_tasks: BackgroundTasks, token: str, request_id: str):
    if google_sheets is None:
        raise HTTPException(status_code=500, detail="GOOGLE_SHEETS_WEBHOOK_URL is not configured")
    if config.google_sheets_token is None:
        raise HTTPException(status_code=500, detail="GOOGLE_SHEETS_TOKEN is not configured")
    if token != config.google_sheets_token:
        raise HTTPException(status_code=401, detail="Invalid token")

    background_tasks.add_task(_analyze_and_send_to_sheets, token, request_id)
    return {"status": "accepted", "request_id": request_id}


@app.post("/sheets")
async def new_column_in_sheet(req: Request):
    payload = await req.json()

    time_add = payload.get("timestamp")
    dt = datetime.datetime.strptime(time_add, "%d.%m.%Y %H:%M:%S")
    dt = dt + datetime.timedelta(hours=2)
    ts = dt.timestamp()

    phone = correct_phone(payload.get("phone"))
    fullname = payload.get("fullName")
    description = payload.get("description")
    materials = payload.get("materialsLink", "")

    ok, contact_data = await amo_api.get_contact_by_phone(phone)
    if ok:
        contact_id = contact_data.get("id")
    else:
        await bot.send_message(
            chat_id=config.admin_chat_id,
            text=(
                "Не получилось найти id контакта.\n"
                f"Номер телефона {phone}\n"
                f"ФИО: {fullname}"
            ),
        )
        raise ValueError("Не получилось найти id контакта.")

    resp = await amo_api.add_new_task(
        contact_id=contact_id,
        descr=description,
        url_materials=materials,
        time_value=ts,
    )
    logger.info(resp.status_code)
    return {"status": "ok"}


@app.post("/sheets/marketplace")
async def sheets_marketplace(req: Request):
    payload = await req.json()

    lead_id = int(payload.get("data", {}).get("lead_id"))
    items = payload.get("data", {}).get("items", [])
    items = filter(lambda x: float(x.get("quantity")) >= 1, items)

    result = await amo_api.add_catalog_elements_to_lead(lead_id=lead_id, elements=items)
    logger.info(result)
    return {"status": "ok"}


@app.post("/market/new_order/notification")
async def new_order_from_yandex(req: Request):
    payload = await req.json()
    try:
        order_id = payload.get("orderId")
        await bot.send_message(chat_id=config.admin_chat_id, text=str(payload))

        url_order = (
            f"https://api.partner.market.yandex.ru/v2/campaigns/{config.magazne_id}/orders/{order_id}"
        )
        order_json = await asyncio.to_thread(
            lambda: requests.get(
                url=url_order,
                headers={"Api-Key": config.yandex_api_key},
            ).json()
        )
        order_data = Order(order_data=order_json)
        order_data.get_buyer()

        url_buyer = (
            f"https://api.partner.market.yandex.ru/v2/campaigns/{config.magazne_id}/orders/{order_id}/buyer"
        )
        buyer_json = await asyncio.to_thread(
            lambda: requests.get(
                url=url_buyer,
                headers={"Api-Key": config.yandex_api_key},
            ).json()
        )

        buyer_info = (buyer_json or {}).get("result") or {}
        buyer_phone = buyer_info.get("phone")

        contact_id = await amo_api.create_new_contact(
            first_name=order_data.buyer_firstname,
            last_name=order_data.buyer_lastname,
            phone=buyer_phone,
        )
        logger.info(f"Создан контакт id {contact_id}")

        new_lead = await amo_api.send_lead_to_amo(contact_id=contact_id, order_id=order_id)
        lead_id = (new_lead.get("_embedded") or {}).get("leads", [{}])[0].get("id")
        logger.info(f"Создана новая сделка: {lead_id}")

        await amo_api.add_new_note_to_lead(
            lead_id=lead_id,
            text=order_data.order_items + order_data.address,
            order_id=order_id,
        )

    except Exception as error:
        logger.exception(error)
        logger.error(f"Не получилось обработать вебхук {payload}")
    finally:
        return {
            "version": "1.0.0",
            "name": "Amowebhooks",
            "time": "2025-11-20T11:09:26.246Z",
        }


@app.get("/telegram")
async def education(request: Request):
    logger.info(
        "GET %s | query=%s | cookies=%s | headers=%s",
        str(request.url),
        dict(request.query_params),
        request.cookies,
        {k: v for k, v in request.headers.items()},
    )
    bot_url = config.telegram_bot_url
    qp = request.query_params
    cookies = request.cookies
    sbjs_current = _parse_sourcebuster_cookie(_cookie_value(cookies, "sbjs_current"))
    sbjs_first = _parse_sourcebuster_cookie(_cookie_value(cookies, "sbjs_first"))
    yclid = _get_tracking_value(
        query_params=qp,
        cookies=cookies,
        key="yclid",
        sbjs_current=sbjs_current,
        sbjs_first=sbjs_first,
        sbjs_key="id",
    )

    with SessionLocal() as session:
        if yclid:
            existing_visit = session.execute(
                select(EducationVisit).where(EducationVisit.yclid == yclid)
            ).scalar_one_or_none()

            if existing_visit:
                start_query = urlencode({"start": existing_visit.id})
                return RedirectResponse(f"{bot_url}?{start_query}", status_code=302)

        education_visit = EducationVisit(
            utm_source=_get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_source",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="src",
            ),
            utm_medium=_get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_medium",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="mdm",
            ),
            utm_campaign=_get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_campaign",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="cmp",
            ),
            utm_content=_get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_content",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="cnt",
            ),
            utm_term=_get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_term",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="trm",
            ),
            yclid=yclid,
            cm_id=_normalize_tracking_value(qp.get("cm_id"))
                  or _normalize_tracking_value(_cookie_value(cookies, "cm_id")),
            block=_normalize_tracking_value(qp.get("block"))
                  or _normalize_tracking_value(_cookie_value(cookies, "block")),
        )
        session.add(education_visit)
        session.commit()
        session.refresh(education_visit)

        logger.info(
            f"utm_source={education_visit.utm_source} "
            f"utm_medium={education_visit.utm_medium} "
            f"utm_campaign={education_visit.utm_campaign} "
            f"utm_content={education_visit.utm_content} "
            f"yclid={education_visit.yclid}"
        )

        start_query = urlencode({"start": education_visit.id})
        return RedirectResponse(f"{bot_url}?{start_query}", status_code=302)


@app.get("/get_utm/{record_id}")
async def get_utm(record_id: int, token: str):
    if config.get_utm_token is None:
        raise HTTPException(status_code=500, detail="GET_UTM_TOKEN is not configured")
    if token != config.get_utm_token:
        raise HTTPException(status_code=401, detail="Invalid token")

    with SessionLocal() as session:
        education_visit = session.get(EducationVisit, record_id)
        if education_visit is None:
            raise HTTPException(status_code=404, detail="EducationVisit record not found")

        utm_data = {
            "utm_source": education_visit.utm_source,
            "utm_medium": education_visit.utm_medium,
            "utm_campaign": education_visit.utm_campaign,
            "utm_content": education_visit.utm_content,
            "utm_term": education_visit.utm_term,
            "yclid": education_visit.yclid,
        }

        if all(value in (None, "") for value in utm_data.values()):
            raise HTTPException(status_code=404, detail="UTM tags not found")

        return utm_data
