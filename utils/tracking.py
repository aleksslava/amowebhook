from collections.abc import Mapping
from urllib.parse import unquote_plus


def get_cookie_value(cookies: Mapping[str, str], key: str) -> str | None:
    value = cookies.get(key)
    if value is not None:
        return value
    for cookie_key, cookie_value in cookies.items():
        if cookie_key.strip() == key:
            return cookie_value
    return None


def normalize_tracking_value(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if cleaned.lower() in {"(none)", "none", "null", "undefined"}:
        return None
    return cleaned


def parse_sourcebuster_cookie(cookie_value: str | None) -> dict[str, str]:
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


def get_tracking_value(
        query_params: Mapping[str, str],
        cookies: Mapping[str, str],
        key: str,
        sbjs_current: Mapping[str, str],
        sbjs_first: Mapping[str, str],
        sbjs_key: str,
        cookie_keys: tuple[str, ...] | None = None,
) -> str | None:
    value = normalize_tracking_value(query_params.get(key))
    if value is not None:
        return value

    keys_to_check = cookie_keys or (key,)
    for cookie_key in keys_to_check:
        value = normalize_tracking_value(get_cookie_value(cookies, cookie_key))
        if value is not None:
            return value

    value = normalize_tracking_value(sbjs_current.get(sbjs_key))
    if value is not None:
        return value

    return normalize_tracking_value(sbjs_first.get(sbjs_key))
