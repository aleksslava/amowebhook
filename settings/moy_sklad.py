from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import dotenv
import httpx


logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
DEFAULT_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})
RETRYABLE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})


class MoySkladAPIError(RuntimeError):
    """Ошибка запроса к JSON API МоегоСклада."""

    def __init__(
        self,
        *,
        status_code: int | None,
        method: str,
        endpoint: str,
        request_id: str | None = None,
        errors: Sequence[Mapping[str, Any]] | None = None,
    ) -> None:
        self.status_code = status_code
        self.method = method
        self.endpoint = endpoint
        self.request_id = request_id
        self.errors = [dict(error) for error in errors or ()]

        messages = [str(error.get("error")) for error in self.errors if error.get("error")]
        detail = "; ".join(messages) if messages else "неизвестная ошибка API"
        status = str(status_code) if status_code is not None else "network"
        request_suffix = f", request_id={request_id}" if request_id else ""
        super().__init__(
            f"MoySklad API error: status={status}, method={method}, "
            f"endpoint={endpoint}{request_suffix}: {detail}"
        )


class MoySkladClient:
    """Асинхронный клиент JSON API 1.2 МоегоСклада."""

    def __init__(
        self,
        token: str | None = None,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if backoff_factor < 0:
            raise ValueError("backoff_factor must be non-negative")

        self.token = token
        self.base_url = base_url.rstrip("/")
        self.timeout = httpx.Timeout(timeout)
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self._transport = transport
        self._client: httpx.AsyncClient | None = None

        parsed_base_url = httpx.URL(self.base_url)
        if not parsed_base_url.is_absolute_url:
            raise ValueError("base_url must be an absolute URL")
        self._base_origin = (
            parsed_base_url.scheme,
            parsed_base_url.host,
            parsed_base_url.port,
        )

    async def __aenter__(self) -> MoySkladClient:
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def open(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=f"{self.base_url}/",
                timeout=self.timeout,
                transport=self._transport,
                headers={
                    "Accept": "application/json;charset=utf-8",
                    "Accept-Encoding": "gzip",
                },
            )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @staticmethod
    def build_query_params(
        *,
        params: Mapping[str, Any] | None = None,
        filters: Sequence[str] | str | None = None,
        expand: Sequence[str] | str | None = None,
        order: Sequence[str] | str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict[str, Any]:
        """Формирует query-параметры в синтаксисе JSON API 1.2."""

        result = dict(params or {})
        generated: dict[str, Any] = {}

        filter_values = MoySkladClient._normalize_query_parts(filters, "filters")
        expand_values = MoySkladClient._normalize_query_parts(expand, "expand")
        order_values = MoySkladClient._normalize_query_parts(order, "order")

        if filter_values:
            generated["filter"] = ";".join(filter_values)
        if expand_values:
            generated["expand"] = ",".join(expand_values)
        if order_values:
            generated["order"] = ";".join(order_values)

        if limit is not None:
            if isinstance(limit, bool) or not 1 <= limit <= 1000:
                raise ValueError("limit must be between 1 and 1000")
            if expand_values and limit > 100:
                raise ValueError("limit cannot exceed 100 when expand is used")
            generated["limit"] = limit

        if offset is not None:
            if isinstance(offset, bool) or offset < 0:
                raise ValueError("offset must be non-negative")
            generated["offset"] = offset

        conflicts = result.keys() & generated.keys()
        if conflicts:
            names = ", ".join(sorted(conflicts))
            raise ValueError(f"query parameters specified more than once: {names}")

        result.update(generated)
        return result

    @staticmethod
    def _normalize_query_parts(
        values: Sequence[str] | str | None,
        name: str,
    ) -> list[str]:
        if values is None:
            return []
        parts = [values] if isinstance(values, str) else list(values)
        if any(not isinstance(part, str) or not part.strip() for part in parts):
            raise ValueError(f"{name} must contain non-empty strings")
        return parts

    async def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
        filters: Sequence[str] | str | None = None,
        expand: Sequence[str] | str | None = None,
        order: Sequence[str] | str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        query_params = self.build_query_params(
            params=params,
            filters=filters,
            expand=expand,
            order=order,
            limit=limit,
            offset=offset,
        )
        return await self._request(
            method,
            endpoint,
            params=query_params,
            json=json,
            authenticated=True,
            allow_post_retry=False,
        )

    async def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
        authenticated: bool,
        auth: httpx.Auth | None = None,
        allow_post_retry: bool = False,
    ) -> Any:
        await self.open()
        assert self._client is not None

        normalized_method = method.upper()
        request_url = self._normalize_endpoint(endpoint)
        safe_endpoint = self._safe_endpoint(endpoint)
        can_retry = normalized_method in RETRYABLE_METHODS or (
            normalized_method == "POST" and allow_post_retry
        )
        attempts = self.max_retries + 1 if can_retry else 1

        request_headers: dict[str, str] = {}
        if authenticated:
            if not self.token:
                raise RuntimeError("MOYSKLAD_TOKEN is not configured")
            request_headers["Authorization"] = f"Bearer {self.token}"

        for attempt in range(attempts):
            started_at = time.monotonic()
            try:
                response = await self._client.request(
                    normalized_method,
                    request_url,
                    params=params,
                    json=json,
                    headers=request_headers,
                    auth=auth,
                )
            except httpx.TransportError:
                elapsed_ms = (time.monotonic() - started_at) * 1000
                logger.warning(
                    "MoySklad request failed method=%s endpoint=%s attempt=%s/%s "
                    "elapsed_ms=%.1f",
                    normalized_method,
                    safe_endpoint,
                    attempt + 1,
                    attempts,
                    elapsed_ms,
                )
                if attempt + 1 >= attempts:
                    raise MoySkladAPIError(
                        status_code=None,
                        method=normalized_method,
                        endpoint=safe_endpoint,
                        errors=[{"error": "network transport error"}],
                    ) from None
                await asyncio.sleep(self._backoff_delay(attempt))
                continue

            elapsed_ms = (time.monotonic() - started_at) * 1000
            logger.info(
                "MoySklad request method=%s endpoint=%s params=%s status=%s "
                "attempt=%s/%s elapsed_ms=%.1f",
                normalized_method,
                safe_endpoint,
                sorted((params or {}).keys()),
                response.status_code,
                attempt + 1,
                attempts,
                elapsed_ms,
            )

            if (
                response.status_code in RETRYABLE_STATUSES
                and attempt + 1 < attempts
            ):
                await asyncio.sleep(self._retry_delay(response, attempt))
                continue

            if response.is_error:
                self._raise_api_error(response, normalized_method, safe_endpoint)

            if response.status_code == 204 or not response.content:
                return {}
            try:
                return response.json()
            except ValueError:
                raise MoySkladAPIError(
                    status_code=response.status_code,
                    method=normalized_method,
                    endpoint=safe_endpoint,
                    request_id=self._response_request_id(response),
                    errors=[{"error": "API returned a non-JSON response"}],
                ) from None

        raise RuntimeError("unreachable request state")

    def _normalize_endpoint(self, endpoint: str) -> str:
        if not endpoint or not endpoint.strip():
            raise ValueError("endpoint must be a non-empty string")

        parsed = httpx.URL(endpoint)
        if parsed.is_absolute_url:
            origin = (parsed.scheme, parsed.host, parsed.port)
            if origin != self._base_origin:
                raise ValueError("refusing to send credentials to an external host")
            return endpoint
        return endpoint.lstrip("/")

    @staticmethod
    def _safe_endpoint(endpoint: str) -> str:
        parsed = urlsplit(endpoint)
        return parsed.path or "/"

    def _backoff_delay(self, retry_index: int) -> float:
        return self.backoff_factor * (2**retry_index)

    def _retry_delay(self, response: httpx.Response, retry_index: int) -> float:
        if response.status_code == 429:
            retry_ms = response.headers.get("X-Lognex-Retry-After")
            if retry_ms:
                try:
                    return max(0.0, float(retry_ms) / 1000)
                except ValueError:
                    pass

            retry_seconds = response.headers.get("Retry-After")
            if retry_seconds:
                try:
                    return max(0.0, float(retry_seconds))
                except ValueError:
                    pass

        return self._backoff_delay(retry_index)

    @staticmethod
    def _response_request_id(response: httpx.Response) -> str | None:
        return response.headers.get("X-Lognex-Request-Id") or response.headers.get(
            "X-Request-Id"
        )

    @classmethod
    def _raise_api_error(
        cls,
        response: httpx.Response,
        method: str,
        endpoint: str,
    ) -> None:
        errors: list[Mapping[str, Any]] = []
        try:
            payload = response.json()
        except ValueError:
            payload = None

        if isinstance(payload, Mapping):
            raw_errors = payload.get("errors")
            if isinstance(raw_errors, list):
                errors = [error for error in raw_errors if isinstance(error, Mapping)]
            elif payload.get("error"):
                errors = [{"error": payload["error"]}]

        if not errors:
            errors = [{"error": "API returned an error response"}]

        raise MoySkladAPIError(
            status_code=response.status_code,
            method=method,
            endpoint=endpoint,
            request_id=cls._response_request_id(response),
            errors=errors,
        )

    async def iter_rows(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        filters: Sequence[str] | str | None = None,
        expand: Sequence[str] | str | None = None,
        order: Sequence[str] | str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> AsyncIterator[dict[str, Any]]:
        page_limit = limit if limit is not None else (100 if expand else 1000)
        current_offset = offset

        while True:
            payload = await self.request(
                "GET",
                endpoint,
                params=params,
                filters=filters,
                expand=expand,
                order=order,
                limit=page_limit,
                offset=current_offset,
            )
            if not isinstance(payload, Mapping) or not isinstance(payload.get("rows"), list):
                raise MoySkladAPIError(
                    status_code=200,
                    method="GET",
                    endpoint=self._safe_endpoint(endpoint),
                    errors=[{"error": "collection response does not contain rows"}],
                )

            rows = payload["rows"]
            for row in rows:
                if not isinstance(row, dict):
                    raise MoySkladAPIError(
                        status_code=200,
                        method="GET",
                        endpoint=self._safe_endpoint(endpoint),
                        errors=[{"error": "collection contains a non-object row"}],
                    )
                yield row

            if not rows:
                break

            current_offset += len(rows)
            meta = payload.get("meta")
            has_next = isinstance(meta, Mapping) and bool(meta.get("nextHref"))
            if not has_next and len(rows) < page_limit:
                break

    async def fetch_processing_order(
        self,
        endpoint: str,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        payload = await self.request(
            "GET",
            endpoint,
            expand=["state", "processingPlan"],
        )
        if not isinstance(payload, dict) or not isinstance(payload.get("id"), str):
            raise MoySkladAPIError(
                status_code=200,
                method="GET",
                endpoint=self._safe_endpoint(endpoint),
                errors=[{"error": "processing order response is not an object"}],
            )

        meta = payload.get("meta")
        if isinstance(meta, Mapping) and meta.get("type") != "processingorder":
            raise MoySkladAPIError(
                status_code=200,
                method="GET",
                endpoint=self._safe_endpoint(endpoint),
                errors=[{"error": "response is not a processing order"}],
            )

        positions = payload.get("positions")
        positions_meta = positions.get("meta") if isinstance(positions, Mapping) else None
        positions_href = (
            positions_meta.get("href") if isinstance(positions_meta, Mapping) else None
        )
        if not isinstance(positions_href, str) or not positions_href:
            raise MoySkladAPIError(
                status_code=200,
                method="GET",
                endpoint=self._safe_endpoint(endpoint),
                errors=[{"error": "processing order does not contain positions href"}],
            )

        position_rows = [
            row
            async for row in self.iter_rows(
                positions_href,
                expand=["assortment"],
            )
        ]
        return payload, position_rows

    async def generate_token(self, login: str, password: str) -> str:
        if not login or not password:
            raise ValueError("MoySklad login and password are required")

        payload = await self._request(
            "POST",
            "security/token",
            authenticated=False,
            auth=httpx.BasicAuth(login, password),
            allow_post_retry=True,
        )
        token = payload.get("access_token") if isinstance(payload, Mapping) else None
        if not isinstance(token, str) or not token:
            raise MoySkladAPIError(
                status_code=200,
                method="POST",
                endpoint="security/token",
                errors=[{"error": "token response does not contain access_token"}],
            )

        self.token = token
        return token

    async def ensure_processing_order_webhooks(
        self,
        callback_url: str,
    ) -> list[dict[str, Any]]:
        self._validate_callback_url(callback_url)
        webhooks = [row async for row in self.iter_rows("entity/webhook")]
        result: list[dict[str, Any]] = []

        for action in ("CREATE", "UPDATE"):
            matching = self._find_webhook(webhooks, callback_url, action)
            desired_diff = "FIELDS" if action == "UPDATE" else None

            if matching is not None:
                update_payload: dict[str, Any] = {}
                if not matching.get("enabled", False):
                    update_payload["enabled"] = True
                if desired_diff and matching.get("diffType") != desired_diff:
                    update_payload["diffType"] = desired_diff

                if update_payload:
                    webhook_id = matching.get("id")
                    if not webhook_id:
                        raise MoySkladAPIError(
                            status_code=200,
                            method="GET",
                            endpoint="entity/webhook",
                            errors=[{"error": "existing webhook does not contain id"}],
                        )
                    matching = await self.request(
                        "PUT",
                        f"entity/webhook/{webhook_id}",
                        json=update_payload,
                    )
                result.append(dict(matching))
                continue

            create_payload: dict[str, Any] = {
                "url": callback_url,
                "action": action,
                "entityType": "processingorder",
            }
            if desired_diff:
                create_payload["diffType"] = desired_diff

            created = await self._create_webhook_idempotently(create_payload)
            webhooks.append(created)
            result.append(created)

        return result

    async def _create_webhook_idempotently(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        callback_url = str(payload["url"])
        action = str(payload["action"])

        for attempt in range(self.max_retries + 1):
            try:
                created = await self.request("POST", "entity/webhook", json=payload)
                if not isinstance(created, dict):
                    raise MoySkladAPIError(
                        status_code=200,
                        method="POST",
                        endpoint="entity/webhook",
                        errors=[{"error": "webhook response is not an object"}],
                    )
                return created
            except MoySkladAPIError as error:
                refreshed = [row async for row in self.iter_rows("entity/webhook")]
                existing = self._find_webhook(refreshed, callback_url, action)
                if existing is not None:
                    return existing

                transient = error.status_code is None or error.status_code in RETRYABLE_STATUSES
                if not transient or attempt >= self.max_retries:
                    raise
                await asyncio.sleep(self._backoff_delay(attempt))

        raise RuntimeError("unreachable webhook creation state")

    @staticmethod
    def _find_webhook(
        webhooks: Sequence[Mapping[str, Any]],
        callback_url: str,
        action: str,
    ) -> dict[str, Any] | None:
        for webhook in webhooks:
            if (
                webhook.get("entityType") == "processingorder"
                and webhook.get("action") == action
                and webhook.get("url") == callback_url
            ):
                return dict(webhook)
        return None

    @staticmethod
    def _validate_callback_url(callback_url: str) -> None:
        parsed = urlsplit(callback_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("callback URL must be an absolute HTTP(S) URL")
        if len(callback_url) > 255:
            raise ValueError("callback URL must not exceed 255 characters")


def save_token_to_env(token: str, env_path: str | Path = DEFAULT_ENV_PATH) -> None:
    if not token:
        raise ValueError("token must be a non-empty string")
    dotenv.set_key(str(env_path), "MOYSKLAD_TOKEN", token)


def _get_config_value(name: str, env_values: Mapping[str, str | None]) -> str | None:
    return os.environ.get(name) or env_values.get(name)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MoySklad JSON API client utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("token", help="generate and save a new access token")

    subscribe_parser = subparsers.add_parser(
        "subscribe",
        help="ensure CREATE and UPDATE processing order webhooks",
    )
    subscribe_parser.add_argument("--url", help="webhook callback URL")
    return parser


async def _run_cli(args: argparse.Namespace, env_path: Path = DEFAULT_ENV_PATH) -> None:
    env_values = dotenv.dotenv_values(env_path)

    if args.command == "token":
        login = _get_config_value("MOYSKLAD_LOGIN", env_values)
        password = _get_config_value("MOYSKLAD_PASSWORD", env_values)
        if not login or not password:
            raise RuntimeError("MOYSKLAD_LOGIN and MOYSKLAD_PASSWORD must be configured")

        logger.warning("Generating a new token revokes previously issued user tokens")
        async with MoySkladClient() as client:
            token = await client.generate_token(login, password)
        save_token_to_env(token, env_path)
        logger.info("MoySklad token was saved to %s", env_path)
        return

    token = _get_config_value("MOYSKLAD_TOKEN", env_values)
    callback_url = args.url or _get_config_value("MOYSKLAD_WEBHOOK_URL", env_values)
    if not token:
        raise RuntimeError("MOYSKLAD_TOKEN must be configured")
    if not callback_url:
        raise RuntimeError("--url or MOYSKLAD_WEBHOOK_URL must be configured")

    async with MoySkladClient(token=token) as client:
        webhooks = await client.ensure_processing_order_webhooks(callback_url)
    logger.info(
        "Processing order webhooks are configured: %s",
        ", ".join(str(webhook.get("action")) for webhook in webhooks),
    )


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(message)s",
    )
    args = _build_parser().parse_args(argv)
    try:
        asyncio.run(_run_cli(args))
    except (MoySkladAPIError, RuntimeError, ValueError, OSError) as error:
        logger.error("MoySklad command failed: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
