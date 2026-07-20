from __future__ import annotations

import hmac
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path
from typing import Callable
from urllib.parse import urlencode

from fastapi import APIRouter, Form, HTTPException, Query, Request, status
from fastapi.responses import FileResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import case, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload, selectinload

from models import MoySkladOrder, OrderItem, User
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
    "delivery",
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


templates.env.filters["datetime"] = _format_datetime
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
        delivery_from_value: str = Query("", alias="delivery_from", max_length=10),
        delivery_to_value: str = Query("", alias="delivery_to", max_length=10),
        sort: str = Query(
            "moment",
            pattern=(
                r"^(?:order|device|processing_plan|performer|state|moment|"
                r"delivery|quantity|readiness)$"
            ),
        ),
        direction: str = Query("desc", pattern=r"^(?:asc|desc)$"),
    ) -> Response:
        moment_from = _parse_optional_date(moment_from_value, "order start date")
        moment_to = _parse_optional_date(moment_to_value, "order end date")
        delivery_from = _parse_optional_date(
            delivery_from_value,
            "production start date",
        )
        delivery_to = _parse_optional_date(
            delivery_to_value,
            "production end date",
        )
        if (
            moment_from is not None
            and moment_to is not None
            and moment_from > moment_to
        ):
            raise HTTPException(status_code=422, detail="Invalid order date range")
        if (
            delivery_from is not None
            and delivery_to is not None
            and delivery_from > delivery_to
        ):
            raise HTTPException(status_code=422, detail="Invalid production date range")

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
            if delivery_from is not None:
                conditions.append(
                    MoySkladOrder.delivery_planned_moment
                    >= datetime.combine(delivery_from, time.min)
                )
            if delivery_to is not None:
                conditions.append(
                    MoySkladOrder.delivery_planned_moment
                    <= datetime.combine(delivery_to, time.max)
                )

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
                "delivery": MoySkladOrder.delivery_planned_moment,
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
            order_readiness = {
                order.id: calculate_order_readiness(order) for order in orders
            }
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
            if delivery_from is not None:
                filter_params["delivery_from"] = delivery_from.isoformat()
            if delivery_to is not None:
                filter_params["delivery_to"] = delivery_to.isoformat()

            current_params = {
                **filter_params,
                "sort": sort,
                "direction": direction,
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
                        "delivery_from": (
                            delivery_from.isoformat() if delivery_from else ""
                        ),
                        "delivery_to": delivery_to.isoformat() if delivery_to else "",
                    },
                    "active_filters": bool(filter_params),
                    "sort": sort,
                    "direction": direction,
                    "sort_urls": sort_urls,
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
                )
            )
            if order is None or (
                not current_user.is_admin and order.user_id != current_user.id
            ):
                raise HTTPException(status_code=404, detail="Order not found")
            order.items.sort(key=lambda item: (item.assortment_name or "", item.id))
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
                    "saved": request.query_params.get("saved") == "1",
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
            order = db.get(MoySkladOrder, order_id)
            if order is None or (
                not current_user.is_admin and order.user_id != current_user.id
            ):
                raise HTTPException(status_code=404, detail="Order not found")
            require_csrf(web_session, csrf_token)
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

            order.produced_quantity = parsed_produced
            for item in items:
                item.spent_quantity = parsed_values[item.moysklad_position_id]
            db.commit()

        return RedirectResponse(
            f"/cabinet/orders/{order_id}?saved=1",
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
