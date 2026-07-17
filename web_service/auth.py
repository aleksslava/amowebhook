from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import secrets
from dataclasses import dataclass
from typing import Any

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer


SESSION_COOKIE = "production_session"
LOGIN_CSRF_COOKIE = "production_login_csrf"
SESSION_MAX_AGE = 12 * 60 * 60
_SCRYPT_N = 2**14
_SCRYPT_R = 8
_SCRYPT_P = 1
_SCRYPT_LENGTH = 64


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    password_hash = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=_SCRYPT_N,
        r=_SCRYPT_R,
        p=_SCRYPT_P,
        dklen=_SCRYPT_LENGTH,
    )
    return "$".join(
        (
            "scrypt",
            str(_SCRYPT_N),
            str(_SCRYPT_R),
            str(_SCRYPT_P),
            base64.urlsafe_b64encode(salt).decode("ascii"),
            base64.urlsafe_b64encode(password_hash).decode("ascii"),
        )
    )


def verify_password(password: str, encoded: str) -> bool:
    try:
        algorithm, n, r, p, salt, expected = encoded.split("$", 5)
        parameters = (int(n), int(r), int(p))
        if algorithm != "scrypt" or parameters != (_SCRYPT_N, _SCRYPT_R, _SCRYPT_P):
            return False
        decoded_salt = base64.urlsafe_b64decode(salt.encode("ascii"))
        decoded_expected = base64.urlsafe_b64decode(expected.encode("ascii"))
        if len(decoded_salt) != 16 or len(decoded_expected) != _SCRYPT_LENGTH:
            return False
        actual = hashlib.scrypt(
            password.encode("utf-8"),
            salt=decoded_salt,
            n=parameters[0],
            r=parameters[1],
            p=parameters[2],
            dklen=_SCRYPT_LENGTH,
        )
        return hmac.compare_digest(actual, decoded_expected)
    except (ValueError, TypeError, binascii.Error):
        return False


@dataclass(frozen=True)
class WebSession:
    user_id: int
    csrf_token: str


class SessionManager:
    def __init__(self, secret: str, *, cookie_secure: bool) -> None:
        self._sessions = URLSafeTimedSerializer(secret, salt="production-web-session")
        self._login_csrf = URLSafeTimedSerializer(secret, salt="production-login-csrf")
        self.cookie_secure = cookie_secure

    def create_session(self, user_id: int) -> tuple[str, WebSession]:
        session = WebSession(user_id=user_id, csrf_token=secrets.token_urlsafe(32))
        value = self._sessions.dumps(
            {"user_id": session.user_id, "csrf_token": session.csrf_token}
        )
        return value, session

    def load_session(self, value: str | None) -> WebSession | None:
        if not value:
            return None
        try:
            payload: Any = self._sessions.loads(value, max_age=SESSION_MAX_AGE)
            user_id = payload.get("user_id")
            csrf_token = payload.get("csrf_token")
            if not isinstance(user_id, int) or not isinstance(csrf_token, str):
                return None
            return WebSession(user_id=user_id, csrf_token=csrf_token)
        except (BadSignature, SignatureExpired, AttributeError):
            return None

    def create_login_csrf(self) -> str:
        return self._login_csrf.dumps(secrets.token_urlsafe(32))

    def valid_login_csrf(self, cookie_value: str | None, form_value: str) -> bool:
        if not cookie_value or not hmac.compare_digest(cookie_value, form_value):
            return False
        try:
            self._login_csrf.loads(cookie_value, max_age=10 * 60)
            return True
        except (BadSignature, SignatureExpired):
            return False
