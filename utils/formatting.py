from decimal import Decimal, InvalidOperation


def format_grouped_number(value: int | float | str | None) -> str:
    if value in (None, ""):
        return ""

    raw_value = str(value).replace(" ", "").replace(",", ".")
    try:
        number = Decimal(raw_value)
    except (InvalidOperation, ValueError):
        return str(value)

    normalized = format(number.normalize(), "f")
    sign = ""
    if normalized.startswith("-"):
        sign = "-"
        normalized = normalized[1:]

    if "." in normalized:
        integer_part, fraction_part = normalized.split(".", 1)
        fraction_part = fraction_part.rstrip("0")
    else:
        integer_part, fraction_part = normalized, ""

    grouped_integer = f"{int(integer_part):,}".replace(",", " ")
    if fraction_part:
        return f"{sign}{grouped_integer},{fraction_part}"
    return f"{sign}{grouped_integer}"
