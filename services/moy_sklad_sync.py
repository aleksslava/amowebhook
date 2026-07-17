from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from models import MoySkladOrder, OrderItem, User
from settings.moy_sklad import MoySkladClient


logger = logging.getLogger(__name__)


class MoySkladWebhookPayloadError(ValueError):
    pass


class MoySkladDataError(ValueError):
    pass


@dataclass(frozen=True)
class OrderSyncResult:
    order_id: int
    created: bool
    stale: bool
    item_count: int
    user_id: int | None


def processing_order_hrefs(payload: Mapping[str, Any]) -> list[str]:
    events = payload.get("events")
    if not isinstance(events, list):
        raise MoySkladWebhookPayloadError("webhook payload must contain events array")

    result: list[str] = []
    seen: set[str] = set()
    for event in events:
        if not isinstance(event, Mapping):
            raise MoySkladWebhookPayloadError("each webhook event must be an object")

        action = event.get("action")
        meta = event.get("meta")
        if action not in {"CREATE", "UPDATE"}:
            logger.warning("Ignoring unsupported MoySklad webhook action=%s", action)
            continue
        if not isinstance(meta, Mapping) or meta.get("type") != "processingorder":
            logger.warning(
                "Ignoring MoySklad webhook event with entity_type=%s",
                meta.get("type") if isinstance(meta, Mapping) else None,
            )
            continue

        href = meta.get("href")
        if not isinstance(href, str) or not href:
            raise MoySkladWebhookPayloadError(
                "processingorder webhook event must contain meta.href"
            )
        if href not in seen:
            seen.add(href)
            result.append(href)

    return result


def extract_performer_name(payload: Mapping[str, Any]) -> str | None:
    attributes = payload.get("attributes")
    if not isinstance(attributes, list):
        return None

    for attribute in attributes:
        if not isinstance(attribute, Mapping) or attribute.get("name") != "Исполнитель":
            continue
        value = attribute.get("value")
        if isinstance(value, str):
            return value or None
        if isinstance(value, Mapping):
            name = value.get("name")
            return name if isinstance(name, str) and name else None
        return None
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise MoySkladDataError("MoySklad datetime value must be a string")
    try:
        return datetime.fromisoformat(value)
    except ValueError as error:
        raise MoySkladDataError(f"Invalid MoySklad datetime: {value}") from error


def _decimal(value: Any, *, required: bool) -> Decimal | None:
    if value is None and not required:
        return None
    if isinstance(value, bool):
        raise MoySkladDataError("MoySklad numeric value must not be boolean")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise MoySkladDataError(f"Invalid MoySklad numeric value: {value}") from error


def _required_string(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value:
        raise MoySkladDataError(f"MoySklad payload must contain {field}")
    return value


def _entity_id(entity: Any) -> str | None:
    if not isinstance(entity, Mapping):
        return None
    entity_id = entity.get("id")
    if isinstance(entity_id, str) and entity_id:
        return entity_id
    meta = entity.get("meta")
    href = meta.get("href") if isinstance(meta, Mapping) else None
    if not isinstance(href, str) or not href:
        return None
    path = urlsplit(href).path.rstrip("/")
    return path.rsplit("/", 1)[-1] if path else None


def _optional_string(payload: Mapping[str, Any], field: str) -> str | None:
    value = payload.get(field)
    return value if isinstance(value, str) else None


def _sync_once(
    session_factory: Callable[[], Session],
    order_payload: Mapping[str, Any],
    positions: Sequence[Mapping[str, Any]],
) -> OrderSyncResult:
    moysklad_id = _required_string(order_payload, "id")
    name = _required_string(order_payload, "name")
    source_updated_at = _parse_datetime(order_payload.get("updated"))
    performer_name = extract_performer_name(order_payload)

    with session_factory() as session, session.begin():
        order = session.scalar(
            select(MoySkladOrder).where(MoySkladOrder.moysklad_id == moysklad_id)
        )
        created = order is None
        if order is not None and (
            order.moysklad_updated_at is not None
            and source_updated_at is not None
            and source_updated_at < order.moysklad_updated_at
        ):
            return OrderSyncResult(
                order_id=order.id,
                created=False,
                stale=True,
                item_count=len(order.items),
                user_id=order.user_id,
            )

        user = None
        if performer_name is not None:
            user = session.scalar(select(User).where(User.name == performer_name))
            if user is None:
                logger.warning(
                    "MoySklad performer was not found order_id=%s performer=%s",
                    moysklad_id,
                    performer_name,
                )
        else:
            logger.warning(
                "MoySklad performer is empty order_id=%s",
                moysklad_id,
            )

        state = order_payload.get("state")
        state_name = state.get("name") if isinstance(state, Mapping) else None
        if order is None:
            order = MoySkladOrder(moysklad_id=moysklad_id, name=name, raw_payload={})
            session.add(order)

        order.user_id = user.id if user is not None else None
        order.name = name
        order.code = _optional_string(order_payload, "code")
        order.external_code = _optional_string(order_payload, "externalCode")
        order.description = _optional_string(order_payload, "description")
        order.moment = _parse_datetime(order_payload.get("moment"))
        order.delivery_planned_moment = _parse_datetime(
            order_payload.get("deliveryPlannedMoment")
        )
        order.moysklad_created_at = _parse_datetime(order_payload.get("created"))
        order.moysklad_updated_at = source_updated_at
        applicable = order_payload.get("applicable")
        order.applicable = applicable if isinstance(applicable, bool) else None
        order.production_quantity = _decimal(
            order_payload.get("quantity"),
            required=False,
        )
        order.performer_name = performer_name
        order.state_id = _entity_id(state)
        order.state_name = state_name if isinstance(state_name, str) else None
        order.raw_payload = dict(order_payload)
        order.synced_at = datetime.utcnow()
        session.flush()

        if not created:
            session.execute(delete(OrderItem).where(OrderItem.order_id == order.id))
            session.flush()

        seen_positions: set[str] = set()
        for position in positions:
            position_id = _required_string(position, "id")
            if position_id in seen_positions:
                raise MoySkladDataError(
                    f"Duplicate MoySklad position id: {position_id}"
                )
            seen_positions.add(position_id)

            assortment = position.get("assortment")
            assortment_name = (
                assortment.get("name") if isinstance(assortment, Mapping) else None
            )
            assortment_code = (
                assortment.get("code") if isinstance(assortment, Mapping) else None
            )
            assortment_meta = (
                assortment.get("meta") if isinstance(assortment, Mapping) else None
            )
            assortment_type = (
                assortment_meta.get("type")
                if isinstance(assortment_meta, Mapping)
                else None
            )
            session.add(
                OrderItem(
                    order_id=order.id,
                    moysklad_position_id=position_id,
                    assortment_id=_entity_id(assortment),
                    assortment_type=(
                        assortment_type if isinstance(assortment_type, str) else None
                    ),
                    assortment_name=(
                        assortment_name if isinstance(assortment_name, str) else None
                    ),
                    assortment_code=(
                        assortment_code if isinstance(assortment_code, str) else None
                    ),
                    quantity=_decimal(position.get("quantity"), required=True),
                    reserve=_decimal(position.get("reserve"), required=False),
                    raw_payload=dict(position),
                )
            )

        session.flush()
        return OrderSyncResult(
            order_id=order.id,
            created=created,
            stale=False,
            item_count=len(positions),
            user_id=order.user_id,
        )


def sync_processing_order(
    session_factory: Callable[[], Session],
    order_payload: Mapping[str, Any],
    positions: Sequence[Mapping[str, Any]],
) -> OrderSyncResult:
    for attempt in range(2):
        try:
            return _sync_once(session_factory, order_payload, positions)
        except IntegrityError:
            if attempt == 1:
                raise
            logger.warning(
                "Retrying concurrent MoySklad order upsert order_id=%s",
                order_payload.get("id"),
            )
    raise RuntimeError("unreachable MoySklad order sync state")


async def process_processing_order_webhook(
    payload: Mapping[str, Any],
    client: MoySkladClient,
    session_factory: Callable[[], Session],
) -> list[OrderSyncResult]:
    results: list[OrderSyncResult] = []
    for href in processing_order_hrefs(payload):
        order_payload, positions = await client.fetch_processing_order(href)
        result = await asyncio.to_thread(
            sync_processing_order,
            session_factory,
            order_payload,
            positions,
        )
        logger.info(
            "MoySklad order synchronized source_id=%s database_id=%s created=%s "
            "stale=%s items=%s user_id=%s",
            order_payload.get("id"),
            result.order_id,
            result.created,
            result.stale,
            result.item_count,
            result.user_id,
        )
        results.append(result)
    return results
