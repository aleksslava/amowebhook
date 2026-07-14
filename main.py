import asyncio
import datetime
import json
import logging
from pathlib import Path
from urllib.parse import parse_qs, urlencode
from uuid import uuid4

import requests
from aiogram import Bot
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.background import BackgroundTask

from models import Base, EducationVisit
from services.test_kp_to_pdf import render_template_to_pdf
from settings.async_amo_api import AmoCRMWrapperAsync
from settings.google_sheets import GoogleSheetsIntegration
from settings.settings import load_config
from utils.analytics import analyze_and_send_to_sheets, analyze_customers_and_send_to_sheets
from utils.files import cleanup_generated_file
from utils.formatting import format_grouped_number
from utils.tracking import (
    get_cookie_value,
    get_tracking_value,
    normalize_tracking_value,
    parse_sourcebuster_cookie,
)
from utils.utils import (
    Order,
    correct_phone,
    get_catalog_elements_from_lead,
    get_items_to_kp,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(filename)s:%(lineno)d #%(levelname)-8s "
           "[%(asctime)s] - %(name)s - %(message)s"
)

config = load_config()
bot = Bot(token=config.tg_bot.token)

app = FastAPI()
templates = Jinja2Templates(directory="services/templates")
KP_TEMPLATE_PATH = Path("services/templates/hite_pro_kp.html")
KP_IMAGE_PATH = KP_TEMPLATE_PATH.with_name("img01.png")
KP_LOGO_PATH = KP_TEMPLATE_PATH.with_name("logo.png")
KP_MONTAGE_IMAGE_PATH = KP_TEMPLATE_PATH.with_name("montage_image.webp")
KP_PDF_TMP_DIR = Path("services/tmp_pdf")
template_dir = KP_TEMPLATE_PATH.resolve().parent

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

google_sheets_customers = (
    GoogleSheetsIntegration(config.google_sheets_customers_webhook_url)
    if config.google_sheets_webhook_url else None
)


templates.env.filters["grouped_number"] = format_grouped_number


@app.on_event("startup")
async def on_startup() -> None:
    Base.metadata.create_all(bind=db_engine)
    await amo_api.open()
    # Обычно init_oauth2() НЕ вызывают на каждый старт, если токены уже сохранены в .env


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await amo_api.close()


@app.get("/analyze")
async def analyze(background_tasks: BackgroundTasks, token: str, request_id: str):
    if google_sheets is None:
        raise HTTPException(status_code=500, detail="GOOGLE_SHEETS_WEBHOOK_URL is not configured")
    if config.google_sheets_token is None:
        raise HTTPException(status_code=500, detail="GOOGLE_SHEETS_TOKEN is not configured")
    if token != config.google_sheets_token:
        raise HTTPException(status_code=401, detail="Invalid token")

    background_tasks.add_task(
        analyze_and_send_to_sheets,
        amo_api=amo_api,
        google_sheets=google_sheets,
        token=token,
        request_id=request_id,
    )
    return {"status": "accepted", "request_id": request_id}

@app.get("/analyze_customers")
async def analyze(background_tasks: BackgroundTasks, token: str, request_id: str):
    if google_sheets is None:
        raise HTTPException(status_code=500, detail="GOOGLE_SHEETS_WEBHOOK_URL is not configured")
    if config.google_sheets_token is None:
        raise HTTPException(status_code=500, detail="GOOGLE_SHEETS_TOKEN is not configured")
    if token != config.google_sheets_token:
        raise HTTPException(status_code=401, detail="Invalid token")

    background_tasks.add_task(
        analyze_customers_and_send_to_sheets,
        amo_api=amo_api,
        google_sheets=google_sheets,
        google_sheets_customers=google_sheets_customers,
        token=token,
        request_id=request_id,
    )
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
    # logger.info(
    #     "GET %s | query=%s | cookies=%s | headers=%s",
    #     str(request.url),
    #     dict(request.query_params),
    #     request.cookies,
    #     {k: v for k, v in request.headers.items()},
    # )
    bot_url = config.telegram_bot_url
    qp = request.query_params
    cookies = request.cookies
    sbjs_current = parse_sourcebuster_cookie(get_cookie_value(cookies, "sbjs_current"))
    sbjs_first = parse_sourcebuster_cookie(get_cookie_value(cookies, "sbjs_first"))
    yclid = get_tracking_value(
        query_params=qp,
        cookies=cookies,
        key="yclid",
        sbjs_current=sbjs_current,
        sbjs_first=sbjs_first,
        sbjs_key="id",
        cookie_keys=("yclid", "_ym_uid"),
    )

    with SessionLocal() as session:
        # if yclid:
        #     existing_visit = session.execute(
        #         select(EducationVisit).where(EducationVisit.yclid == yclid)
        #     ).scalar_one_or_none()
        #
        #     if existing_visit:
        #         start_query = urlencode({"start": existing_visit.id})
        #         return RedirectResponse(f"{bot_url}?{start_query}", status_code=302)

        education_visit = EducationVisit(
            utm_source=get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_source",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="src",
            ),
            utm_medium=get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_medium",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="mdm",
            ),
            utm_campaign=get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_campaign",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="cmp",
            ),
            utm_content=get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_content",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="cnt",
            ),
            utm_term=get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_term",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="trm",
            ),
            yclid=yclid,
            cm_id=normalize_tracking_value(qp.get("cm_id"))
                  or normalize_tracking_value(get_cookie_value(cookies, "cm_id")),
            block=normalize_tracking_value(qp.get("block"))
                  or normalize_tracking_value(get_cookie_value(cookies, "block")),
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
async def get_utm(record_id: int, utm_token: str):
    if config.get_utm_token is None:
        raise HTTPException(status_code=500, detail="GET_UTM_TOKEN is not configured")
    if utm_token != config.get_utm_token:
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
        logger.info(
            f'utm_source={utm_data["utm_source"]}\nutm_medium={utm_data["utm_medium"]}\nutm_campaign={utm_data["utm_campaign"]}\nyclid={utm_data["yclid"]}')

        return utm_data


@app.post("/new_message_tp")
async def proceed_webhook_tp(req: Request):
    raw_body = await req.body()
    content_type = req.headers.get("content-type", "").lower()

    if not raw_body:
        payload = {}
    elif "application/json" in content_type:
        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError:
            payload = raw_body.decode("utf-8", errors="replace")
    elif "application/x-www-form-urlencoded" in content_type:
        payload = parse_qs(raw_body.decode("utf-8", errors="replace"), keep_blank_values=True)
    else:
        text_body = raw_body.decode("utf-8", errors="replace")
        try:
            payload = json.loads(text_body)
        except json.JSONDecodeError:
            payload = text_body

    # logger.info("new_message_tp payload=%s", payload)
    return {'status': 'ok'}



@app.get("/max")
async def education_max(request: Request):
    bot_url = config.max_bot_url
    qp = request.query_params
    cookies = request.cookies
    sbjs_current = parse_sourcebuster_cookie(get_cookie_value(cookies, "sbjs_current"))
    sbjs_first = parse_sourcebuster_cookie(get_cookie_value(cookies, "sbjs_first"))
    yclid = get_tracking_value(
        query_params=qp,
        cookies=cookies,
        key="yclid",
        sbjs_current=sbjs_current,
        sbjs_first=sbjs_first,
        sbjs_key="id",
        cookie_keys=("yclid", "_ym_uid"),
    )

    with SessionLocal() as session:

        education_visit = EducationVisit(
            utm_source=get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_source",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="src",
            ),
            utm_medium=get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_medium",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="mdm",
            ),
            utm_campaign=get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_campaign",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="cmp",
            ),
            utm_content=get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_content",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="cnt",
            ),
            utm_term=get_tracking_value(
                query_params=qp,
                cookies=cookies,
                key="utm_term",
                sbjs_current=sbjs_current,
                sbjs_first=sbjs_first,
                sbjs_key="trm",
            ),
            yclid=yclid,
            cm_id=normalize_tracking_value(qp.get("cm_id"))
                  or normalize_tracking_value(get_cookie_value(cookies, "cm_id")),
            block=normalize_tracking_value(qp.get("block"))
                  or normalize_tracking_value(get_cookie_value(cookies, "block")),
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




@app.get("/kp/assets/img01.png", include_in_schema=False, name="kp_image")
async def get_kp_image() -> FileResponse:
    if not KP_IMAGE_PATH.exists():
        raise HTTPException(status_code=404, detail="KP image not found")
    return FileResponse(KP_IMAGE_PATH)


@app.get("/kp/assets/logo.png", include_in_schema=False, name="kp_logo")
async def get_kp_logo() -> FileResponse:
    if not KP_LOGO_PATH.exists():
        raise HTTPException(status_code=404, detail="KP logo not found")
    return FileResponse(KP_LOGO_PATH)


@app.get("/kp/assets/montage_image.webp", include_in_schema=False, name="kp_montage_image")
async def get_kp_montage_image() -> FileResponse:
    if not KP_MONTAGE_IMAGE_PATH.exists():
        raise HTTPException(status_code=404, detail="KP montage image not found")
    return FileResponse(KP_MONTAGE_IMAGE_PATH)


@app.get("/kp")
async def get_kp(request: Request, lead_id: int):
    project_field_id = 938609
    discount_field_id = 972024
    delivery_field_id = 972028
    lead_response = await amo_api.get_lead_with_catalog_elements(lead_id=lead_id)
    lead_catalog_elements = get_catalog_elements_from_lead(lead_response)
    project = amo_api._get_custom_field_value(lead_response, project_field_id)
    discount = int(amo_api._get_custom_field_value(lead_response, discount_field_id))
    responsible_manager_id = lead_response.get("responsible_user_id")
    responsible_manager = await amo_api.get_responsible_user_by_id(responsible_manager_id)
    responsible_manager_name = responsible_manager.get('name')
    delivery = int(amo_api._get_custom_field_value(lead_response, delivery_field_id))

    if not lead_catalog_elements:
        raise HTTPException(status_code=404, detail="В сделке нет элементов каталога")

    catalog_id = None
    lead_embedded_elements = lead_response.get("_embedded", {}).get("catalog_elements", [])
    for element in lead_embedded_elements:
        metadata = element.get("metadata") or {}
        current_catalog_id = metadata.get("catalog_id")
        if current_catalog_id is not None:
            catalog_id = int(current_catalog_id)
            break

    if catalog_id is None:
        raise HTTPException(status_code=400, detail="Не найден catalog_id в элементах сделки")

    catalogs_elements_response = await amo_api.get_catalogs_elements(
        catalog_id=catalog_id,
        elements=lead_catalog_elements,
    )
    products = get_items_to_kp(catalogs_elements_response, lead_catalog_elements, discount=discount)
    if delivery > 0:
        products.append({
            "name": 'Доставка',
            'price': delivery,
            'discount': 0,
            'quantity': 1,
            'total_discount': delivery,
            'total': delivery,
        })

    total_amount_value = 0.0
    total_amount_value_discount = 0.0
    for product in products:
        try:
            total_amount_value += float(product.get("total", 0))
            total_amount_value_discount += float(product.get("total_discount", 0))
        except (TypeError, ValueError):
            continue

    total_amount = int(total_amount_value) if total_amount_value.is_integer() else total_amount_value
    total_amount_discount = int(total_amount_value_discount) if total_amount_value_discount.is_integer() else total_amount_value_discount
    today = datetime.date.today()
    proposal_date = today.strftime("%d.%m.%Y")
    valid_until = (today + datetime.timedelta(days=14)).strftime("%d.%m.%Y")

    image_names = ["slide_2.webp", "slide_1.webp"]  # имена файлов из папки services/templates
    content_blocks = [
        {
            "type": "image",
            "src": (template_dir / name).resolve().as_uri(),
            "alt": Path(name).stem,
        }
        for name in image_names
        if (template_dir / name).exists()
    ]

    context = {
        "request": request,
        "proposal_number": lead_id,
        "proposal_date": proposal_date,
        "valid_until": valid_until,
        "client_name": f"Сделка № {lead_id}",
        "company_name": "ООО «ХАЙТ ПРО ИНЖИНИРИНГ»",
        "manager_name": responsible_manager_name,
        "manager_email": "sales@hite-pro.ru",
        "manager_phone": "+7 (495) 256-33-00",
        "products": products,
        "total_amount": total_amount,
        "total_discount": total_amount_discount,
        'lead_id': lead_id,
        'project': project,
        "content_blocks": content_blocks,
    }
    return templates.TemplateResponse("hite_pro_kp.html", context)


@app.get("/kp/pdf")
async def get_kp_pdf(request: Request, lead_id: int) -> FileResponse:
    kp_html_response = await get_kp(request=request, lead_id=lead_id)
    context = dict(kp_html_response.context)
    context.pop("request", None)

    KP_PDF_TMP_DIR.mkdir(parents=True, exist_ok=True)
    pdf_output_path = (KP_PDF_TMP_DIR / f"kp_{lead_id}_{uuid4().hex}.pdf").resolve()
    try:
        render_template_to_pdf(
            template_path=KP_TEMPLATE_PATH,
            context=context,
            output_pdf_path=pdf_output_path,
        )
    except Exception as error:
        cleanup_generated_file(pdf_output_path)
        logger.exception(f"Failed to generate KP PDF for lead_id={lead_id}: {error}")
        raise HTTPException(status_code=500, detail="Failed to generate PDF")

    return FileResponse(
        path=pdf_output_path,
        media_type="application/pdf",
        filename=f"КП HiTE PRO №{lead_id} от {datetime.date.today():%d.%m.%Y}.pdf",
        content_disposition_type="inline",
        background=BackgroundTask(cleanup_generated_file, pdf_output_path),
    )

@app.get("/kp/service")
async def get_service_kp(request: Request):
    return templates.TemplateResponse("service_kp.html", {"request": request})

@app.get("/kp/partner")
async def get_partner_kp(request: Request) -> FileResponse:
    raw_context = request.query_params.get("context") or request.query_params.get("contex")
    if not raw_context:
        raise HTTPException(status_code=400, detail="context query parameter is required")

    try:
        context = json.loads(raw_context)
    except json.JSONDecodeError as error:
        raise HTTPException(status_code=400, detail="context must be valid JSON") from error

    if not isinstance(context, dict):
        raise HTTPException(status_code=400, detail="context must be a JSON object")

    context.pop("request", None)
    lead_id = str(context.get("lead_id") or context.get("proposal_number") or "partner")
    safe_lead_id = "".join(
        char if char.isalnum() or char in {"-", "_"} else "_"
        for char in lead_id
    ) or "partner"

    KP_PDF_TMP_DIR.mkdir(parents=True, exist_ok=True)
    pdf_output_path = (KP_PDF_TMP_DIR / f"kp_partner_{safe_lead_id}_{uuid4().hex}.pdf").resolve()
    try:
        render_template_to_pdf(
            template_path=KP_TEMPLATE_PATH,
            context=context,
            output_pdf_path=pdf_output_path,
        )
    except Exception as error:
        cleanup_generated_file(pdf_output_path)
        logger.exception(f"Failed to generate partner KP PDF: {error}")
        raise HTTPException(status_code=500, detail="Failed to generate PDF")

    return FileResponse(
        path=pdf_output_path,
        media_type="application/pdf",
        filename=f"КП HiTE PRO №{lead_id} от {datetime.date.today():%d.%m.%Y}.pdf",
        content_disposition_type="inline",
        background=BackgroundTask(cleanup_generated_file, pdf_output_path),
    )


