import asyncio
import datetime
import logging
from decimal import Decimal, InvalidOperation
from typing import Any

from settings.async_amo_api import build_amo_results, build_amo_results_analize_customers
from utils.utils import conver_timestamp_to_days, convert_data


logger = logging.getLogger(__name__)


def build_leads_payload(amo_results) -> list[dict[str, Any]]:
    return [
        {
            "lead_id": result.lead_obj.lead_id,
            "lead_price": result.lead_obj.lead_price,
            "created_at": convert_data(result.lead_obj.created_at),
            "close_at": convert_data(result.lead_obj.close_at),
            "shipment_at": convert_data(result.lead_obj.shipment_at),
            "attestate_at": convert_data(result.customer_obj.created_at),
            "contact_id": result.lead_obj.contact_id,
            "customer_id": result.customer_obj.customer_id,
            "clean_price": result.lead_obj.clean_price,
            "last_buy": conver_timestamp_to_days(result.lead_obj.last_buy),
            "time_from_attestate": conver_timestamp_to_days(result.lead_obj.time_from_attestate),
            "paid_at": convert_data(result.lead_obj.paid_at),
        }
        for result in amo_results
    ]


def build_customers_analysis_payload(amo_results_customers) -> list[dict[str, Any]]:
    def _lead_price_value(value) -> Decimal:
        if value in (None, ""):
            return Decimal("0")
        try:
            price = Decimal(str(value).replace(" ", "").replace(",", "."))
        except (InvalidOperation, ValueError):
            return Decimal("0")
        if not price.is_finite():
            return Decimal("0")
        return price

    def _json_amount(value: Decimal) -> int | float:
        if value == value.to_integral_value():
            return int(value)
        return float(value)

    def _paid_at_datetime(value) -> datetime.datetime | None:
        if value in (None, 0, ""):
            return None
        try:
            return datetime.datetime.fromtimestamp(float(value))
        except (TypeError, ValueError, OverflowError, OSError):
            return None

    periods = {
        "clean_budjet_1": (
            datetime.datetime(2023, 7, 1, 0, 0, 0),
            datetime.datetime(2026, 6, 30, 23, 59, 59),
        ),
        "clean_budjet_2": (
            datetime.datetime(2024, 7, 1, 0, 0, 0),
            datetime.datetime(2026, 6, 30, 23, 59, 59),
        ),
        "clean_budjet_3": (
            datetime.datetime(2025, 7, 1, 0, 0, 0),
            datetime.datetime(2026, 6, 30, 23, 59, 59),
        ),
        "clean_budjet_4": (
            datetime.datetime(2026, 1, 1, 0, 0, 0),
            datetime.datetime(2026, 6, 30, 23, 59, 59),
        ),
        "clean_budjet_5": (
            datetime.datetime(2026, 4, 1, 0, 0, 0),
            datetime.datetime(2026, 6, 30, 23, 59, 59),
        ),
    }

    payload = []
    for result in amo_results_customers:
        lead_list = result.lead_list
        clean_budjet = Decimal("0")
        period_budjets = {key: Decimal("0") for key in periods}

        for lead in lead_list:
            lead_price = _lead_price_value(lead.lead_price)
            clean_budjet += lead_price

            paid_at = _paid_at_datetime(lead.paid_at)
            if paid_at is None:
                continue

            for key, (start_at, end_at) in periods.items():
                if start_at <= paid_at <= end_at:
                    period_budjets[key] += lead_price

        payload.append(
            {
                "customer_id": result.customer_obj.customer_id,
                "status": result.customer_obj.status,
                "leads_count": len(lead_list),
                "clean_budjet": _json_amount(clean_budjet),
                "clean_budjet_1": _json_amount(period_budjets["clean_budjet_1"]),
                "clean_budjet_2": _json_amount(period_budjets["clean_budjet_2"]),
                "clean_budjet_3": _json_amount(period_budjets["clean_budjet_3"]),
                "clean_budjet_4": _json_amount(period_budjets["clean_budjet_4"]),
                "clean_budjet_5": _json_amount(period_budjets["clean_budjet_5"]),
            }
        )

    return payload


async def analyze_and_send_to_sheets(
        *,
        amo_api,
        google_sheets,
        token: str,
        request_id: str,
) -> None:
    try:
        if google_sheets is None:
            logger.error("GOOGLE_SHEETS_WEBHOOK_URL is not configured")
            return

        leads_list, customers_list = await asyncio.gather(
            amo_api.get_pipeline_1628622_status_142_leads(),
            amo_api.get_customers_with_contacts(),
        )

        amo_results = build_amo_results(leads=leads_list, customers=customers_list)
        payload = build_leads_payload(amo_results)

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


async def analyze_customers_and_send_to_sheets(
        *,
        amo_api,
        google_sheets,
        google_sheets_customers,
        token: str,
        request_id: str,
) -> None:
    try:
        if google_sheets is None:
            logger.error("GOOGLE_SHEETS_WEBHOOK_URL is not configured")
            return

        leads_list, customers_list = await asyncio.gather(
            amo_api.get_pipeline_1628622_status_142_leads(),
            amo_api.get_customers_with_contacts(),
        )

        amo_results = build_amo_results_analize_customers(
            leads=leads_list,
            customers=customers_list,
        )
        payload = build_customers_analysis_payload(amo_results)

        response = await asyncio.to_thread(
            google_sheets_customers.send_json,
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
