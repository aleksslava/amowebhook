from __future__ import annotations

import hmac
import math
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable

from fastapi import APIRouter, Form, HTTPException, Query, Request, status
from fastapi.responses import FileResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
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


def _format_datetime(value: datetime | None) -> str:
    return value.strftime("%d.%m.%Y %H:%M") if value else "—"


def _format_number(value: Decimal | None) -> str:
    if value is None:
        return "—"
    return f"{value:f}".rstrip("0").rstrip(".") or "0"


templates.env.filters["datetime"] = _format_datetime
templates.env.filters["number"] = _format_number


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
        user_id: int | None = Query(None, ge=1),
    ) -> Response:
        with session_factory() as db:
            auth = require_user(request, db)
            if isinstance(auth, Response):
                return auth
            current_user, web_session = auth

            conditions = []
            selected_user = None
            if current_user.is_admin:
                if user_id is not None:
                    selected_user = db.get(User, user_id)
                    if selected_user is None:
                        raise HTTPException(status_code=404, detail="User not found")
                    conditions.append(MoySkladOrder.user_id == user_id)
            else:
                conditions.append(MoySkladOrder.user_id == current_user.id)

            count_query = select(func.count(MoySkladOrder.id))
            orders_query = (
                select(MoySkladOrder)
                .options(joinedload(MoySkladOrder.user))
                .order_by(MoySkladOrder.moment.desc(), MoySkladOrder.id.desc())
                .offset((page - 1) * PAGE_SIZE)
                .limit(PAGE_SIZE)
            )
            if conditions:
                count_query = count_query.where(*conditions)
                orders_query = orders_query.where(*conditions)

            total = db.scalar(count_query) or 0
            orders = list(db.scalars(orders_query))
            users = (
                list(db.scalars(select(User).order_by(User.name)))
                if current_user.is_admin
                else []
            )
            return template(
                request,
                "orders.html",
                {
                    "current_user": current_user,
                    "csrf_token": web_session.csrf_token,
                    "orders": orders,
                    "users": users,
                    "selected_user": selected_user,
                    "selected_user_id": user_id,
                    "page": page,
                    "total": total,
                    "total_pages": max(1, math.ceil(total / PAGE_SIZE)),
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
            return template(
                request,
                "order_detail.html",
                {
                    "current_user": current_user,
                    "csrf_token": web_session.csrf_token,
                    "order": order,
                },
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
