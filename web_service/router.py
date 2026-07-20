from __future__ import annotations

import hmac
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path
from typing import Callable
from urllib.parse import urlencode, urlsplit

from fastapi import APIRouter, Form, HTTPException, Query, Request, status
from fastapi.responses import FileResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import case, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload, selectinload

from models import MoySkladOrder, OrderItem, OrderSuborder, User
from web_service.auth import (
    LOGIN_CSRF_COOKIE,
    SESSION_COOKIE,
    SESSION_MAX_AGE,
    SessionManager,
    WebSession,
    hash_password,
    verify_password,
)


PACKAGE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(PACKAGE_DIR / "templates"))
PAGE_SIZE = 25
MAX_LOCAL_QUANTITY = 999_999_999_999
MAX_BATCH_SUBORDERS = 1000
_INTEGER_PATTERN = re.compile(r"^[0-9]+$")
_DATE_PATTERN = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_NO_STATE_FILTER = "__none__"
_ORDER_SORT_KEYS = {
    "order",
    "device",
    "processing_plan",
    "performer",
    "state",
    "moment",
    "next_stage",
    "quantity",
    "readiness",
}
_ORDER_STATUS_CLASSES = {
    "Готово": "ready",
    "В работе": "in-work",
    "К комплектовке": "picking",
    "Планируется": "planned",
    "Резерв": "reserve",
    "Ремонт": "repair",
    "Пауза": "paused",
    "Ожидание": "waiting",
}


@dataclass(frozen=True)
class Readiness:
    label: str
    width: str
    complete: bool


def _format_datetime(value: datetime | None) -> str:
    return value.strftime("%d.%m.%Y %H:%M") if value else "—"


def _format_date(value: date | None) -> str:
    return value.strftime("%d.%m.%Y") if value else "—"


def _format_number(value: Decimal | None) -> str:
    if value is None:
        return "—"
    formatted = f"{value:f}"
    return formatted.rstrip("0").rstrip(".") if "." in formatted else formatted


def _order_status_class(value: str | None) -> str:
    return _ORDER_STATUS_CLASSES.get(value, "neutral")


def calculate_readiness(completed: Decimal, quantity: Decimal) -> Readiness:
    if quantity <= 0:
        percent = Decimal("0")
    else:
        percent = completed / quantity * Decimal("100")
    rounded = percent.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    width = min(max(percent, Decimal("0")), Decimal("100"))
    return Readiness(
        label=f"{_format_number(rounded)}%",
        width=_format_number(width),
        complete=percent >= 100,
    )


def calculate_order_readiness(order: MoySkladOrder) -> Readiness:
    return calculate_readiness(
        order.produced_quantity or Decimal("0"),
        order.production_quantity or Decimal("0"),
    )


def _parse_local_quantity(value: str, field_name: str) -> Decimal:
    if not _INTEGER_PATTERN.fullmatch(value):
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} must be a non-negative integer",
        )
    normalized = value.lstrip("0") or "0"
    if len(normalized) > 12:
        raise HTTPException(status_code=400, detail=f"{field_name} is too large")
    parsed = int(normalized)
    if parsed > MAX_LOCAL_QUANTITY:
        raise HTTPException(status_code=400, detail=f"{field_name} is too large")
    return Decimal(parsed)


def _spent_limit(quantity: Decimal) -> Decimal:
    if quantity <= 0:
        return Decimal("0")
    return quantity.to_integral_value(rounding=ROUND_FLOOR)


def _orders_url(params: dict[str, str | int]) -> str:
    query = urlencode(params)
    return f"/cabinet/orders?{query}" if query else "/cabinet/orders"


def _safe_orders_return_url(value: str, fallback: str) -> str:
    if not value:
        return fallback
    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc or parsed.path != "/cabinet/orders":
        return fallback
    return value


def _casefold(value: str | None) -> str | None:
    return value.casefold() if value is not None else None


def _case_insensitive_contains(column, value: str, dialect_name: str):
    if dialect_name == "sqlite":
        return func.unicode_casefold(column).contains(
            value.casefold(),
            autoescape=True,
        )
    return column.icontains(value, autoescape=True)


def _parse_optional_date(value: str, field_name: str) -> date | None:
    value = value.strip()
    if not value:
        return None
    if not _DATE_PATTERN.fullmatch(value):
        raise HTTPException(status_code=422, detail=f"Invalid {field_name}")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid {field_name}",
        ) from error


def _parse_required_date(value: str, field_name: str) -> date:
    parsed = _parse_optional_date(value, field_name)
    if parsed is None:
        raise HTTPException(status_code=422, detail=f"Invalid {field_name}")
    return parsed


templates.env.filters["datetime"] = _format_datetime
templates.env.filters["date"] = _format_date
templates.env.filters["number"] = _format_number
templates.env.filters["order_status_class"] = _order_status_class


def create_web_router(
    session_factory: Callable[[], Session],
    *,
    session_secret: str | None,
    cookie_secure: bool,
) -> APIRouter:
    router = APIRouter(prefix="/cabinet", tags=["production-cabinet"])
    sessions = (
        SessionManager(session_secret, cookie_secure=cookie_secure)
        if session_secret
        else None
    )

    def template(
        request: Request,
        name: str,
        context: dict,
        *,
        status_code: int = 200,
    ) -> Response:
        response = templates.TemplateResponse(
            request=request,
            name=name,
            context=context,
            status_code=status_code,
        )
        response.headers["Cache-Control"] = "no-store"
        return response

    def authenticated(request: Request, db: Session) -> tuple[User, WebSession] | None:
        if sessions is None:
            return None
        web_session = sessions.load_session(request.cookies.get(SESSION_COOKIE))
        if web_session is None:
            return None
        user = db.get(User, web_session.user_id)
        if user is None or not user.is_active:
            return None
        return user, web_session

    def require_user(request: Request, db: Session) -> tuple[User, WebSession] | Response:
        result = authenticated(request, db)
        if result is None:
            response = RedirectResponse("/cabinet/login", status_code=status.HTTP_303_SEE_OTHER)
            response.delete_cookie(SESSION_COOKIE, path="/cabinet")
            return response
        return result

    def require_csrf(web_session: WebSession, csrf_token: str) -> None:
        if not hmac.compare_digest(web_session.csrf_token, csrf_token):
            raise HTTPException(status_code=400, detail="Invalid CSRF token")

    def accessible_order(
        db: Session,
        current_user: User,
        order_id: int,
    ) -> MoySkladOrder:
        order = db.get(MoySkladOrder, order_id)
        if order is None or (
            not current_user.is_admin and order.user_id != current_user.id
        ):
            raise HTTPException(status_code=404, detail="Order not found")
        return order

    def admin_order(db: Session, current_user: User, order_id: int) -> MoySkladOrder:
        if not current_user.is_admin:
            raise HTTPException(
                status_code=403,
                detail="Administrator access required",
            )
        order = db.get(MoySkladOrder, order_id)
        if order is None:
            raise HTTPException(status_code=404, detail="Order not found")
        return order

    def sync_suborder_actuals(db: Session, order: MoySkladOrder) -> None:
        db.flush()
        count, total = db.execute(
            select(
                func.count(OrderSuborder.id),
                func.coalesce(func.sum(OrderSuborder.actual_quantity), 0),
            ).where(OrderSuborder.order_id == order.id)
        ).one()
        if count:
            order.produced_quantity = total

    @router.get("/assets/app.css", include_in_schema=False)
    def stylesheet() -> FileResponse:
        return FileResponse(
            PACKAGE_DIR / "static" / "app.css",
            media_type="text/css",
            headers={"Cache-Control": "public, max-age=3600"},
        )

    @router.get("", include_in_schema=False)
    def cabinet_root() -> RedirectResponse:
        return RedirectResponse("/cabinet/orders", status_code=status.HTTP_302_FOUND)

    @router.get("/login", include_in_schema=False)
    def login_page(request: Request) -> Response:
        if sessions is None:
            raise HTTPException(status_code=503, detail="WEB_SESSION_SECRET is not configured")
        with session_factory() as db:
            if authenticated(request, db) is not None:
                return RedirectResponse("/cabinet/orders", status_code=status.HTTP_302_FOUND)
        csrf_token = sessions.create_login_csrf()
        response = template(
            request,
            "login.html",
            {"csrf_token": csrf_token, "error": None, "name": ""},
        )
        response.set_cookie(
            LOGIN_CSRF_COOKIE,
            csrf_token,
            max_age=10 * 60,
            httponly=True,
            secure=sessions.cookie_secure,
            samesite="lax",
            path="/cabinet/login",
        )
        return response

    @router.post("/login", include_in_schema=False)
    def login(
        request: Request,
        name: str = Form(...),
        password: str = Form(...),
        csrf_token: str = Form(...),
    ) -> Response:
        if sessions is None:
            raise HTTPException(status_code=503, detail="WEB_SESSION_SECRET is not configured")
        if not sessions.valid_login_csrf(
            request.cookies.get(LOGIN_CSRF_COOKIE), csrf_token
        ):
            raise HTTPException(status_code=400, detail="Invalid CSRF token")

        clean_name = name.strip()
        with session_factory() as db:
            user = (
                db.scalar(select(User).where(User.name == clean_name))
                if 0 < len(clean_name) <= 255 and len(password) <= 128
                else None
            )
            if user is None or not user.is_active or not verify_password(password, user.password_hash):
                return template(
                    request,
                    "login.html",
                    {
                        "csrf_token": csrf_token,
                        "error": "Неверный логин или пароль",
                        "name": name,
                    },
                    status_code=401,
                )
            session_cookie, _ = sessions.create_session(user.id)

        response = RedirectResponse("/cabinet/orders", status_code=status.HTTP_303_SEE_OTHER)
        response.delete_cookie(LOGIN_CSRF_COOKIE, path="/cabinet/login")
        response.set_cookie(
            SESSION_COOKIE,
            session_cookie,
            max_age=SESSION_MAX_AGE,
            httponly=True,
            secure=sessions.cookie_secure,
            samesite="lax",
            path="/cabinet",
        )
        return response

    @router.post("/logout", include_in_schema=False)
    def logout(request: Request, csrf_token: str = Form(...)) -> Response:
        with session_factory() as db:
            auth = authenticated(request, db)
            if auth is not None:
                require_csrf(auth[1], csrf_token)
        response = RedirectResponse("/cabinet/login", status_code=status.HTTP_303_SEE_OTHER)
        response.delete_cookie(SESSION_COOKIE, path="/cabinet")
        return response

    @router.get("/orders", include_in_schema=False)
    def order_list(
        request: Request,
        page: int = Query(1, ge=1),
        user_id: str | None = Query(None, pattern=r"^(?:|0*[1-9][0-9]*)$"),
        q: str = Query("", max_length=255),
        device: str = Query("", max_length=255),
        processing_plan: str = Query("", max_length=255),
        state: str = Query("", max_length=255),
        moment_from_value: str = Query("", alias="moment_from", max_length=10),
        moment_to_value: str = Query("", alias="moment_to", max_length=10),
        next_stage_from_value: str = Query(
            "",
            alias="next_stage_from",
            max_length=10,
        ),
        next_stage_to_value: str = Query(
            "",
            alias="next_stage_to",
            max_length=10,
        ),
        expanded_order_id: int | None = Query(None, alias="expanded", ge=1),
        sort: str = Query(
            "moment",
            pattern=(
                r"^(?:order|device|processing_plan|performer|state|moment|"
                r"next_stage|quantity|readiness)$"
            ),
        ),
        direction: str = Query("desc", pattern=r"^(?:asc|desc)$"),
    ) -> Response:
        moment_from = _parse_optional_date(moment_from_value, "order start date")
        moment_to = _parse_optional_date(moment_to_value, "order end date")
        next_stage_from = _parse_optional_date(
            next_stage_from_value,
            "next stage start date",
        )
        next_stage_to = _parse_optional_date(
            next_stage_to_value,
            "next stage end date",
        )
        if (
            moment_from is not None
            and moment_to is not None
            and moment_from > moment_to
        ):
            raise HTTPException(status_code=422, detail="Invalid order date range")
        if next_stage_from is not None and next_stage_to is not None:
            if next_stage_from > next_stage_to:
                raise HTTPException(
                    status_code=422,
                    detail="Invalid next stage date range",
                )

        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            dialect_name = db.get_bind().dialect.name
            if dialect_name == "sqlite":
                driver_connection = db.connection().connection.driver_connection
                driver_connection.create_function(
                    "unicode_casefold",
                    1,
                    _casefold,
                    deterministic=True,
                )
            selected_user_id = int(user_id) if user_id else None
            q = q.strip()
            device = device.strip()
            processing_plan = processing_plan.strip()
            state = state.strip()
            next_stage_date_expression = (
                select(func.min(OrderSuborder.planned_date))
                .where(
                    OrderSuborder.order_id == MoySkladOrder.id,
                    OrderSuborder.actual_quantity
                    < OrderSuborder.planned_quantity,
                )
                .correlate(MoySkladOrder)
                .scalar_subquery()
            )

            access_conditions = []
            selected_user = None
            if current_user.is_admin:
                if selected_user_id is not None:
                    selected_user = db.get(User, selected_user_id)
                    if selected_user is None:
                        raise HTTPException(status_code=404, detail="User not found")
                    access_conditions.append(MoySkladOrder.user_id == selected_user_id)
            else:
                selected_user_id = None
                access_conditions.append(MoySkladOrder.user_id == current_user.id)

            states_query = (
                select(MoySkladOrder.state_name)
                .distinct()
                .order_by(MoySkladOrder.state_name)
            )
            if access_conditions:
                states_query = states_query.where(*access_conditions)
            available_states = list(db.scalars(states_query))
            state_options = [value for value in available_states if value]
            has_orders_without_state = any(not value for value in available_states)

            conditions = list(access_conditions)
            if q:
                conditions.append(
                    or_(
                        _case_insensitive_contains(
                            MoySkladOrder.name,
                            q,
                            dialect_name,
                        ),
                        _case_insensitive_contains(
                            MoySkladOrder.code,
                            q,
                            dialect_name,
                        ),
                    )
                )
            if device:
                conditions.append(
                    _case_insensitive_contains(
                        MoySkladOrder.device_name,
                        device,
                        dialect_name,
                    )
                )
            if processing_plan:
                conditions.append(
                    _case_insensitive_contains(
                        MoySkladOrder.processing_plan_name,
                        processing_plan,
                        dialect_name,
                    )
                )
            if state == _NO_STATE_FILTER:
                conditions.append(
                    or_(
                        MoySkladOrder.state_name.is_(None),
                        MoySkladOrder.state_name == "",
                    )
                )
            elif state:
                conditions.append(MoySkladOrder.state_name == state)
            if moment_from is not None:
                conditions.append(
                    MoySkladOrder.moment >= datetime.combine(moment_from, time.min)
                )
            if moment_to is not None:
                conditions.append(
                    MoySkladOrder.moment <= datetime.combine(moment_to, time.max)
                )
            if next_stage_from is not None:
                conditions.append(next_stage_date_expression >= next_stage_from)
            if next_stage_to is not None:
                conditions.append(next_stage_date_expression <= next_stage_to)

            readiness_expression = case(
                (
                    func.coalesce(MoySkladOrder.production_quantity, 0) <= 0,
                    0,
                ),
                else_=(
                    func.coalesce(MoySkladOrder.produced_quantity, 0)
                    / MoySkladOrder.production_quantity
                ),
            )
            sort_expressions = {
                "order": MoySkladOrder.name,
                "device": MoySkladOrder.device_name,
                "processing_plan": MoySkladOrder.processing_plan_name,
                "performer": func.coalesce(User.name, MoySkladOrder.performer_name),
                "state": MoySkladOrder.state_name,
                "moment": MoySkladOrder.moment,
                "next_stage": next_stage_date_expression,
                "quantity": MoySkladOrder.production_quantity,
                "readiness": readiness_expression,
            }
            sort_expression = sort_expressions[sort]
            sort_clause = (
                sort_expression.asc() if direction == "asc" else sort_expression.desc()
            )
            if sort not in {"order", "readiness"}:
                sort_clause = sort_clause.nulls_last()

            count_query = select(func.count(MoySkladOrder.id))
            orders_query = select(MoySkladOrder).options(
                joinedload(MoySkladOrder.user)
            )
            if sort == "performer":
                orders_query = orders_query.outerjoin(
                    User,
                    MoySkladOrder.user_id == User.id,
                )
            if conditions:
                count_query = count_query.where(*conditions)
                orders_query = orders_query.where(*conditions)
            orders_query = (
                orders_query.order_by(sort_clause, MoySkladOrder.id.desc())
                .offset((page - 1) * PAGE_SIZE)
                .limit(PAGE_SIZE)
            )

            total = db.scalar(count_query) or 0
            orders = list(db.scalars(orders_query))
            order_ids = [order.id for order in orders]
            suborder_counts: dict[int, int] = {}
            next_stage_dates: dict[int, date] = {}
            if order_ids:
                summary_rows = db.execute(
                    select(
                        OrderSuborder.order_id,
                        func.count(OrderSuborder.id),
                        func.min(
                            case(
                                (
                                    OrderSuborder.actual_quantity
                                    < OrderSuborder.planned_quantity,
                                    OrderSuborder.planned_date,
                                ),
                            )
                        ),
                    )
                    .where(OrderSuborder.order_id.in_(order_ids))
                    .group_by(OrderSuborder.order_id)
                )
                for order_id, count, next_stage_date in summary_rows:
                    suborder_counts[order_id] = count
                    if next_stage_date is not None:
                        next_stage_dates[order_id] = next_stage_date

            expanded_order = next(
                (
                    order
                    for order in orders
                    if order.id == expanded_order_id
                ),
                None,
            )
            expanded_suborders = []
            expanded_suborder_readiness = {}
            if expanded_order is not None:
                expanded_suborders = list(
                    db.scalars(
                        select(OrderSuborder)
                        .where(OrderSuborder.order_id == expanded_order.id)
                        .order_by(OrderSuborder.number)
                    )
                )
                expanded_suborder_readiness = {
                    suborder.id: calculate_readiness(
                        suborder.actual_quantity,
                        suborder.planned_quantity,
                    )
                    for suborder in expanded_suborders
                }
            order_readiness = {
                order.id: calculate_order_readiness(order) for order in orders
            }
            split_disabled_reasons: dict[int, str] = {}
            if current_user.is_admin:
                for order in orders:
                    quantity = order.production_quantity
                    produced = order.produced_quantity or Decimal("0")
                    if quantity is None or quantity <= 0:
                        split_disabled_reasons[order.id] = "Не указан объём заказа"
                    elif quantity != quantity.to_integral_value():
                        split_disabled_reasons[order.id] = "Объём заказа не целый"
                    elif quantity > MAX_LOCAL_QUANTITY:
                        split_disabled_reasons[order.id] = "Объём заказа слишком большой"
                    elif produced < 0 or produced != produced.to_integral_value():
                        split_disabled_reasons[order.id] = "Фактический объём не целый"
                    elif produced > MAX_LOCAL_QUANTITY:
                        split_disabled_reasons[order.id] = "Фактический объём слишком большой"
            users = (
                list(db.scalars(select(User).order_by(User.name)))
                if current_user.is_admin
                else []
            )

            filter_params: dict[str, str | int] = {}
            if q:
                filter_params["q"] = q
            if device:
                filter_params["device"] = device
            if processing_plan:
                filter_params["processing_plan"] = processing_plan
            if state:
                filter_params["state"] = state
            if current_user.is_admin and selected_user_id is not None:
                filter_params["user_id"] = selected_user_id
            if moment_from is not None:
                filter_params["moment_from"] = moment_from.isoformat()
            if moment_to is not None:
                filter_params["moment_to"] = moment_to.isoformat()
            if next_stage_from is not None:
                filter_params["next_stage_from"] = next_stage_from.isoformat()
            if next_stage_to is not None:
                filter_params["next_stage_to"] = next_stage_to.isoformat()

            current_params = {
                **filter_params,
                "sort": sort,
                "direction": direction,
            }
            page_params = {
                **current_params,
                **({"page": page} if page > 1 else {}),
            }
            collapse_url = _orders_url(page_params)
            expand_urls = {
                order.id: _orders_url(
                    {**page_params, "expanded": order.id}
                )
                for order in orders
            }
            sort_urls = {}
            for key in _ORDER_SORT_KEYS:
                next_direction = (
                    "desc" if sort == key and direction == "asc" else "asc"
                )
                sort_urls[key] = _orders_url(
                    {
                        **filter_params,
                        "sort": key,
                        "direction": next_direction,
                    }
                )
            total_pages = max(1, math.ceil(total / PAGE_SIZE))
            return template(
                request,
                "orders.html",
                {
                    "current_user": current_user,
                    "csrf_token": web_session.csrf_token,
                    "orders": orders,
                    "order_readiness": order_readiness,
                    "users": users,
                    "state_options": state_options,
                    "has_orders_without_state": has_orders_without_state,
                    "no_state_filter": _NO_STATE_FILTER,
                    "selected_user": selected_user,
                    "selected_user_id": selected_user_id,
                    "filters": {
                        "q": q,
                        "device": device,
                        "processing_plan": processing_plan,
                        "state": state,
                        "moment_from": moment_from.isoformat() if moment_from else "",
                        "moment_to": moment_to.isoformat() if moment_to else "",
                        "next_stage_from": (
                            next_stage_from.isoformat() if next_stage_from else ""
                        ),
                        "next_stage_to": (
                            next_stage_to.isoformat() if next_stage_to else ""
                        ),
                    },
                    "active_filters": bool(filter_params),
                    "sort": sort,
                    "direction": direction,
                    "sort_urls": sort_urls,
                    "suborder_counts": suborder_counts,
                    "next_stage_dates": next_stage_dates,
                    "expanded_order_id": (
                        expanded_order.id if expanded_order is not None else None
                    ),
                    "expanded_suborders": expanded_suborders,
                    "expanded_suborder_readiness": expanded_suborder_readiness,
                    "expand_urls": expand_urls,
                    "collapse_url": collapse_url,
                    "expanded_return_url": (
                        expand_urls[expanded_order.id]
                        if expanded_order is not None
                        else collapse_url
                    ),
                    "split_disabled_reasons": split_disabled_reasons,
                    "page": page,
                    "total": total,
                    "total_pages": total_pages,
                    "previous_page_url": (
                        _orders_url({**current_params, "page": page - 1})
                        if page > 1
                        else None
                    ),
                    "next_page_url": (
                        _orders_url({**current_params, "page": page + 1})
                        if page < total_pages
                        else None
                    ),
                },
            )

    @router.get("/orders/{order_id}", include_in_schema=False)
    def order_detail(request: Request, order_id: int) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            order = db.scalar(
                select(MoySkladOrder)
                .where(MoySkladOrder.id == order_id)
                .options(
                    joinedload(MoySkladOrder.user),
                    selectinload(MoySkladOrder.items),
                    selectinload(MoySkladOrder.suborders),
                )
            )
            if order is None or (
                not current_user.is_admin and order.user_id != current_user.id
            ):
                raise HTTPException(status_code=404, detail="Order not found")
            order.items.sort(key=lambda item: (item.assortment_name or "", item.id))
            order.suborders.sort(key=lambda suborder: suborder.number)
            spent_limits = {
                item.id: _spent_limit(item.quantity) for item in order.items
            }
            return template(
                request,
                "order_detail.html",
                {
                    "current_user": current_user,
                    "csrf_token": web_session.csrf_token,
                    "order": order,
                    "spent_limits": spent_limits,
                    "suborder_planned_total": sum(
                        (
                            suborder.planned_quantity
                            for suborder in order.suborders
                        ),
                        Decimal("0"),
                    ),
                    "suborder_actual_total": sum(
                        (
                            suborder.actual_quantity
                            for suborder in order.suborders
                        ),
                        Decimal("0"),
                    ),
                    "saved": request.query_params.get("saved") == "1",
                    "suborder_saved": (
                        request.query_params.get("suborder_saved") == "1"
                    ),
                    "suborder_deleted": (
                        request.query_params.get("suborder_deleted") == "1"
                    ),
                },
            )

    @router.post("/orders/{order_id}/production", include_in_schema=False)
    def save_production_quantities(
        request: Request,
        order_id: int,
        produced_quantity: str = Form(""),
        position_id: list[str] = Form([]),
        spent_quantity: list[str] = Form([]),
        csrf_token: str = Form(...),
    ) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            order = accessible_order(db, current_user, order_id)
            require_csrf(web_session, csrf_token)
            has_suborders = bool(
                db.scalar(
                    select(func.count(OrderSuborder.id)).where(
                        OrderSuborder.order_id == order.id
                    )
                )
            )
            parsed_produced = None
            if not has_suborders:
                parsed_produced = _parse_local_quantity(
                    produced_quantity,
                    "Produced quantity",
                )

            if len(position_id) != len(spent_quantity):
                raise HTTPException(status_code=400, detail="Invalid item values")
            if len(set(position_id)) != len(position_id):
                raise HTTPException(status_code=400, detail="Duplicate position id")

            parsed_values: dict[str, Decimal] = {}
            for source_id, value in zip(position_id, spent_quantity):
                parsed_values[source_id] = _parse_local_quantity(
                    value,
                    "Spent quantity",
                )

            items = []
            if position_id:
                items = list(
                    db.scalars(
                        select(OrderItem).where(
                            OrderItem.order_id == order.id,
                            OrderItem.moysklad_position_id.in_(position_id),
                        )
                    )
                )
            if len(items) != len(position_id):
                raise HTTPException(
                    status_code=409,
                    detail="Order positions changed; reload the page",
                )
            for item in items:
                parsed_spent = parsed_values[item.moysklad_position_id]
                if parsed_spent > _spent_limit(item.quantity):
                    raise HTTPException(
                        status_code=400,
                        detail="Spent quantity exceeds item quantity",
                    )

            if has_suborders:
                sync_suborder_actuals(db, order)
            else:
                order.produced_quantity = parsed_produced
            for item in items:
                item.spent_quantity = parsed_values[item.moysklad_position_id]
            db.commit()

        return RedirectResponse(
            f"/cabinet/orders/{order_id}?saved=1",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    @router.post("/orders/{order_id}/suborders", include_in_schema=False)
    def create_suborder(
        request: Request,
        order_id: int,
        planned_quantity: str = Form(""),
        actual_quantity: str = Form(""),
        planned_date: str = Form(""),
        csrf_token: str = Form(...),
    ) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            order = admin_order(db, current_user, order_id)
            require_csrf(web_session, csrf_token)
            parsed_planned = _parse_local_quantity(
                planned_quantity,
                "Planned quantity",
            )
            parsed_actual = _parse_local_quantity(
                actual_quantity,
                "Actual quantity",
            )
            parsed_date = _parse_required_date(planned_date, "planned date")
            next_number = order.last_suborder_number + 1
            order.last_suborder_number = next_number
            db.add(
                OrderSuborder(
                    order_id=order.id,
                    number=next_number,
                    planned_quantity=parsed_planned,
                    actual_quantity=parsed_actual,
                    planned_date=parsed_date,
                )
            )
            sync_suborder_actuals(db, order)
            try:
                db.commit()
            except IntegrityError as error:
                db.rollback()
                raise HTTPException(
                    status_code=409,
                    detail="Suborder number changed; reload the page",
                ) from error

        return RedirectResponse(
            f"/cabinet/orders/{order_id}?suborder_saved=1",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    @router.post(
        "/orders/{order_id}/suborders/split",
        include_in_schema=False,
    )
    def split_order_into_suborders(
        request: Request,
        order_id: int,
        stage_quantity: str = Form(""),
        return_url: str = Form(""),
        csrf_token: str = Form(...),
    ) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            order = admin_order(db, current_user, order_id)
            require_csrf(web_session, csrf_token)

            parsed_stage_quantity = _parse_local_quantity(
                stage_quantity,
                "Stage quantity",
            )
            if parsed_stage_quantity <= 0:
                raise HTTPException(
                    status_code=400,
                    detail="Stage quantity must be positive",
                )
            total_quantity = order.production_quantity
            if total_quantity is None or total_quantity <= 0:
                raise HTTPException(
                    status_code=400,
                    detail="Order quantity must be positive",
                )
            if (
                total_quantity != total_quantity.to_integral_value()
                or total_quantity > MAX_LOCAL_QUANTITY
            ):
                raise HTTPException(
                    status_code=400,
                    detail="Order quantity must be a supported integer",
                )
            produced_quantity = order.produced_quantity or Decimal("0")
            if (
                produced_quantity < 0
                or produced_quantity != produced_quantity.to_integral_value()
                or produced_quantity > MAX_LOCAL_QUANTITY
            ):
                raise HTTPException(
                    status_code=400,
                    detail="Produced quantity must be a supported integer",
                )
            if db.scalar(
                select(func.count(OrderSuborder.id)).where(
                    OrderSuborder.order_id == order.id
                )
            ):
                raise HTTPException(
                    status_code=409,
                    detail="Order already has suborders",
                )

            total_value = int(total_quantity)
            stage_value = int(parsed_stage_quantity)
            stage_count = (total_value + stage_value - 1) // stage_value
            if stage_count > MAX_BATCH_SUBORDERS:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Too many suborders; increase the stage quantity"
                    ),
                )

            remaining_plan = total_value
            remaining_actual = int(produced_quantity)
            suborders = []
            planned_date = date.today()
            for offset in range(1, stage_count + 1):
                planned_value = min(stage_value, remaining_plan)
                actual_value = min(remaining_actual, planned_value)
                suborders.append(
                    OrderSuborder(
                        order_id=order.id,
                        number=order.last_suborder_number + offset,
                        planned_quantity=Decimal(planned_value),
                        actual_quantity=Decimal(actual_value),
                        planned_date=planned_date,
                    )
                )
                remaining_plan -= planned_value
                remaining_actual -= actual_value
            if remaining_actual:
                suborders[-1].actual_quantity += Decimal(remaining_actual)

            order.last_suborder_number += stage_count
            db.add_all(suborders)
            try:
                sync_suborder_actuals(db, order)
                db.commit()
            except IntegrityError as error:
                db.rollback()
                raise HTTPException(
                    status_code=409,
                    detail="Order stages changed; reload the page",
                ) from error

        target = _safe_orders_return_url(
            return_url,
            f"/cabinet/orders?expanded={order_id}",
        )
        return RedirectResponse(
            target,
            status_code=status.HTTP_303_SEE_OTHER,
        )

    @router.post(
        "/orders/{order_id}/suborders/{suborder_id}",
        include_in_schema=False,
    )
    def update_suborder(
        request: Request,
        order_id: int,
        suborder_id: int,
        planned_quantity: str = Form(""),
        actual_quantity: str = Form(""),
        planned_date: str = Form(""),
        return_url: str = Form(""),
        csrf_token: str = Form(...),
    ) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            order = admin_order(db, current_user, order_id)
            require_csrf(web_session, csrf_token)
            suborder = db.scalar(
                select(OrderSuborder).where(
                    OrderSuborder.id == suborder_id,
                    OrderSuborder.order_id == order.id,
                )
            )
            if suborder is None:
                raise HTTPException(status_code=404, detail="Suborder not found")
            suborder.planned_quantity = _parse_local_quantity(
                planned_quantity,
                "Planned quantity",
            )
            suborder.actual_quantity = _parse_local_quantity(
                actual_quantity,
                "Actual quantity",
            )
            suborder.planned_date = _parse_required_date(
                planned_date,
                "planned date",
            )
            sync_suborder_actuals(db, order)
            db.commit()

        target = _safe_orders_return_url(
            return_url,
            f"/cabinet/orders/{order_id}?suborder_saved=1",
        )
        return RedirectResponse(
            target,
            status_code=status.HTTP_303_SEE_OTHER,
        )

    @router.post(
        "/orders/{order_id}/suborders/{suborder_id}/actual",
        include_in_schema=False,
    )
    def update_suborder_actual(
        request: Request,
        order_id: int,
        suborder_id: int,
        actual_quantity: str = Form(""),
        return_url: str = Form(""),
        csrf_token: str = Form(...),
    ) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            order = accessible_order(db, current_user, order_id)
            require_csrf(web_session, csrf_token)
            suborder = db.scalar(
                select(OrderSuborder).where(
                    OrderSuborder.id == suborder_id,
                    OrderSuborder.order_id == order.id,
                )
            )
            if suborder is None:
                raise HTTPException(status_code=404, detail="Suborder not found")
            suborder.actual_quantity = _parse_local_quantity(
                actual_quantity,
                "Actual quantity",
            )
            sync_suborder_actuals(db, order)
            db.commit()

        target = _safe_orders_return_url(
            return_url,
            f"/cabinet/orders/{order_id}?suborder_saved=1",
        )
        return RedirectResponse(
            target,
            status_code=status.HTTP_303_SEE_OTHER,
        )

    @router.post(
        "/orders/{order_id}/suborders/{suborder_id}/delete",
        include_in_schema=False,
    )
    def delete_suborder(
        request: Request,
        order_id: int,
        suborder_id: int,
        csrf_token: str = Form(...),
    ) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            order = admin_order(db, current_user, order_id)
            require_csrf(web_session, csrf_token)
            suborder = db.scalar(
                select(OrderSuborder).where(
                    OrderSuborder.id == suborder_id,
                    OrderSuborder.order_id == order.id,
                )
            )
            if suborder is None:
                raise HTTPException(status_code=404, detail="Suborder not found")
            db.delete(suborder)
            db.flush()
            sync_suborder_actuals(db, order)
            db.commit()

        return RedirectResponse(
            f"/cabinet/orders/{order_id}?suborder_deleted=1",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    @router.get("/admin/users", include_in_schema=False)
    def user_list(request: Request) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            if not current_user.is_admin:
                raise HTTPException(status_code=403, detail="Administrator access required")
            rows = db.execute(
                select(User, func.count(MoySkladOrder.id))
                .outerjoin(MoySkladOrder, MoySkladOrder.user_id == User.id)
                .group_by(User.id)
                .order_by(User.name)
            ).all()
            return template(
                request,
                "users.html",
                {
                    "current_user": current_user,
                    "csrf_token": web_session.csrf_token,
                    "user_rows": rows,
                    "created": request.query_params.get("created") == "1",
                    "disabled": request.query_params.get("disabled") == "1",
                    "password_changed": (
                        request.query_params.get("password_changed") == "1"
                    ),
                },
            )

    @router.get("/admin/users/new", include_in_schema=False)
    def new_user_page(request: Request) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            if not current_user.is_admin:
                raise HTTPException(status_code=403, detail="Administrator access required")
            return template(
                request,
                "user_form.html",
                {
                    "current_user": current_user,
                    "csrf_token": web_session.csrf_token,
                    "error": None,
                    "name": "",
                    "is_admin": False,
                },
            )

    @router.get("/admin/users/{user_id}/password", include_in_schema=False)
    def change_password_page(request: Request, user_id: int) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            if not current_user.is_admin:
                raise HTTPException(status_code=403, detail="Administrator access required")
            user = db.get(User, user_id)
            if user is None:
                raise HTTPException(status_code=404, detail="User not found")
            return template(
                request,
                "password_form.html",
                {
                    "current_user": current_user,
                    "csrf_token": web_session.csrf_token,
                    "user": user,
                    "error": None,
                },
            )

    @router.post("/admin/users/{user_id}/password", include_in_schema=False)
    def change_password(
        request: Request,
        user_id: int,
        password: str = Form(""),
        password_confirmation: str = Form(""),
        csrf_token: str = Form(...),
    ) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            if not current_user.is_admin:
                raise HTTPException(status_code=403, detail="Administrator access required")
            require_csrf(web_session, csrf_token)
            user = db.get(User, user_id)
            if user is None:
                raise HTTPException(status_code=404, detail="User not found")

            error = None
            if not 1 <= len(password) <= 128:
                error = "Пароль должен содержать от 1 до 128 символов"
            elif password != password_confirmation:
                error = "Пароли не совпадают"

            if error is not None:
                return template(
                    request,
                    "password_form.html",
                    {
                        "current_user": current_user,
                        "csrf_token": web_session.csrf_token,
                        "user": user,
                        "error": error,
                    },
                    status_code=400,
                )

            user.password_hash = hash_password(password)
            db.commit()

        return RedirectResponse(
            "/cabinet/admin/users?password_changed=1",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    @router.post("/admin/users", include_in_schema=False)
    def create_user(
        request: Request,
        name: str = Form(...),
        password: str = Form(""),
        password_confirmation: str = Form(""),
        is_admin: bool = Form(False),
        csrf_token: str = Form(...),
    ) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            if not current_user.is_admin:
                raise HTTPException(status_code=403, detail="Administrator access required")
            require_csrf(web_session, csrf_token)

            clean_name = name.strip()
            error = None
            if not clean_name or len(clean_name) > 255:
                error = "Логин должен содержать от 1 до 255 символов"
            elif not 1 <= len(password) <= 128:
                error = "Пароль должен содержать от 1 до 128 символов"
            elif password != password_confirmation:
                error = "Пароли не совпадают"
            elif db.scalar(select(User.id).where(User.name == clean_name)) is not None:
                error = "Пользователь с таким логином уже существует"

            if error is None:
                db.add(
                    User(
                        name=clean_name,
                        password_hash=hash_password(password),
                        is_admin=is_admin,
                        is_active=True,
                    )
                )
                try:
                    db.commit()
                except IntegrityError:
                    db.rollback()
                    error = "Пользователь с таким логином уже существует"
            if error is not None:
                return template(
                    request,
                    "user_form.html",
                    {
                        "current_user": current_user,
                        "csrf_token": web_session.csrf_token,
                        "error": error,
                        "name": clean_name,
                        "is_admin": is_admin,
                    },
                    status_code=400,
                )

        return RedirectResponse(
            "/cabinet/admin/users?created=1",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    @router.post("/admin/users/{user_id}/disable", include_in_schema=False)
    def disable_user(
        request: Request,
        user_id: int,
        csrf_token: str = Form(...),
    ) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth
            if not current_user.is_admin:
                raise HTTPException(status_code=403, detail="Administrator access required")
            require_csrf(web_session, csrf_token)
            if user_id == current_user.id:
                raise HTTPException(status_code=400, detail="Administrator cannot disable itself")
            user = db.get(User, user_id)
            if user is None:
                raise HTTPException(status_code=404, detail="User not found")
            user.is_active = False
            db.commit()
        return RedirectResponse(
            "/cabinet/admin/users?disabled=1",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    return router
