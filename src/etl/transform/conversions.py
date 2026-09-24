"""Metadata-driven scalar conversions used by the standardization stage.

The functions in this module deliberately know nothing about a dataset.  A
function's registry entry describes its output names, while reference data
describes local conventions such as dial plans and abbreviations.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any, Callable, Iterable

import pandas as pd

from etl.core.types import coerce_value, is_null, normalize_null


class ConversionError(ValueError):
    """Raised when a non-empty value cannot be converted."""


@dataclass(frozen=True)
class ConversionContext:
    function: str
    options: dict[str, Any]
    registry: dict[str, dict[str, Any]]
    reference_data: dict[str, Any]
    null_tokens: tuple[str, ...] = ()


def _text(value: Any) -> str:
    return str(value).strip()


def _empty(value: Any, context: ConversionContext) -> bool:
    return is_null(value, context.null_tokens) or _text(value) == ""


def _email(value: Any, context: ConversionContext) -> dict[str, Any]:
    text = _text(value).lower()
    if context.options.get("strip_plus_tag"):
        local, separator, domain = text.partition("@")
        if separator:
            local = local.split("+", 1)[0]
            text = f"{local}@{domain}"
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]{2,}", text):
        raise ConversionError("invalid email address")
    return {"email": text, "email_domain": text.rsplit("@", 1)[1]}


def _telephone(value: Any, context: ConversionContext) -> dict[str, Any]:
    plans = context.reference_data.get("dial_plans", {})
    plan_name = context.options.get("dial_plan")
    plan = plans.get(plan_name, {})
    if not plan:
        raise ConversionError(f"unknown dial plan '{plan_name}'")
    text = _text(value)
    strip = plan.get("strip_characters", " -().")
    number = re.sub(f"[{re.escape(strip)}]", "", text)
    country_code = str(plan.get("country_code", ""))
    if len(set(number.lstrip("+"))) == 1:
        raise ConversionError("placeholder telephone number")
    if number.startswith("+"):
        number = number[1:]
        national = number[len(country_code):] if country_code and number.startswith(country_code) else number
    elif number.startswith("00") and country_code and number[2:].startswith(country_code):
        national = number[2 + len(country_code):]
    elif country_code and number.startswith(country_code):
        national = number[len(country_code):]
    else:
        trunk = str(plan.get("trunk_prefix", ""))
        if trunk and number.startswith(trunk):
            national = number[len(trunk):]
        else:
            national = number
    for old, new in plan.get("legacy_prefixes", {}).items():
        if national.startswith(str(old)):
            national = str(new) + national[len(str(old)):]
            break
    lengths = {int(length) for length in plan.get("national_lengths", [])}
    if not national.isdigit() or (lengths and len(national) not in lengths):
        raise ConversionError("invalid national telephone number")
    return {
        "phone": f"+{country_code}{national}",
        "phone_country_code": country_code,
        "phone_number": national,
    }


def _name(value: Any, context: ConversionContext) -> dict[str, Any]:
    text = re.sub(r"\s+", " ", _text(value))
    rules = context.reference_data.get("name", {})
    honorifics = {str(item).lower().rstrip(".") for item in rules.get("honorifics", [])}
    parts = text.split(" ")
    if parts and parts[0].lower().rstrip(".") in honorifics:
        parts = parts[1:]
    if not parts:
        raise ConversionError("name has no tokens after honorific removal")
    order = context.options.get("name_order", "given_first")

    def display(part: str) -> str:
        value = part.title()
        return re.sub(r"^Mc([a-z])", lambda match: "Mc" + match.group(1).upper(), value)

    if order == "family_first":
        last, first, middle = parts[0], (parts[-1] if len(parts) > 1 else None), parts[1:-1]
    elif order == "given_first":
        first = parts[0]
        last_start = len(parts) - 1
        particles = {str(item).lower() for item in rules.get("particles", [])}
        while last_start > 1 and parts[last_start - 1].lower() in particles:
            last_start -= 1
        last, middle = " ".join(parts[last_start:]), parts[1:last_start]
    else:
        raise ConversionError(f"unsupported name order '{order}'")
    return {
        "full_name": " ".join(display(part) for part in parts),
        "full_name_last": " ".join(display(part) for part in last.split()) if last else None,
        "full_name_first": display(first) if first else None,
        "full_name_middle": " ".join(display(part) for part in middle) if middle else None,
    }


def _address(value: Any, context: ConversionContext) -> dict[str, Any]:
    abbreviations = context.reference_data.get("address_abbreviations", {})
    text = re.sub(r"\s+", " ", _text(value)).strip(" ,")

    def replace(match: re.Match[str]) -> str:
        token = match.group(0)
        replacement = str(abbreviations.get(token.lower().rstrip("."), token))
        return replacement if replacement != token else token.title()

    text = re.sub(r"\b[\w.]+\b", replace, text)
    text = re.sub(r"\s*,\s*", ", ", text)
    if not text:
        raise ConversionError("address is empty after cleanup")
    return {"address_line": text}


def _generic(value: Any, context: ConversionContext) -> dict[str, Any]:
    function = context.function
    if function in {"normalize_null", "null_normalize"}:
        return {context.options.get("output", context.options.get("column", "value")): normalize_null(value, context.null_tokens)}
    if function in {"upper", "lower", "title"}:
        text = _text(value)
        return {context.options.get("output", context.options.get("column", "value")): getattr(text, function)()}
    if function in {"map_values", "lookup", "lookup_value"}:
        mapping_name = context.options.get("mapping") or context.options.get("reference")
        mapping = context.reference_data.get(mapping_name, {})
        key = _text(value).lower() if context.options.get("case_insensitive", True) else value
        normalized = {str(k).lower() if context.options.get("case_insensitive", True) else k: v for k, v in mapping.items()}
        if key not in normalized:
            raise ConversionError(f"value {value!r} is not in lookup '{mapping_name}'")
        return {context.options.get("output", context.options.get("column", "value")): normalized[key]}
    if function in {"to_date", "date"}:
        return {context.options.get("output", context.options.get("column", "value")): _parse_temporal(value, "date", context)}
    if function in {"to_timestamp", "timestamp"}:
        return {context.options.get("output", context.options.get("column", "value")): _parse_temporal(value, "timestamp", context)}
    if function in {"to_boolean", "boolean"}:
        values = context.reference_data.get("booleans", {})
        lowered = _text(value).lower()
        for result, accepted in values.items():
            if lowered in {str(item).lower() for item in accepted}:
                return {context.options.get("output", context.options.get("column", "value")): str(result).lower() == "true"}
        return {context.options.get("output", context.options.get("column", "value")): coerce_value(value, "boolean")}
    if function == "literal":
        return {context.options.get("output", context.options.get("column", "value")): context.options.get("value")}
    raise ConversionError(f"unsupported conversion function '{function}'")


def _parse_temporal(value: Any, logical_type: str, context: ConversionContext) -> Any:
    formats = context.reference_data.get(f"{logical_type}_formats", [])
    parsed = None
    for fmt in formats:
        try:
            parsed = datetime.strptime(_text(value), fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        parsed = coerce_value(value, logical_type)
    if logical_type == "date":
        return parsed.date() if isinstance(parsed, datetime) else parsed
    timezone_name = context.options.get("source_timezone")
    if timezone_name and getattr(parsed, "tzinfo", None) is None:
        parsed = pd.Timestamp(parsed).tz_localize(timezone_name).tz_convert("UTC").to_pydatetime()
    return parsed


CONVERSIONS: dict[str, Callable[[Any, ConversionContext], dict[str, Any]]] = {
    "conv_email": _email,
    "conv_tel": _telephone,
    "conv_name": _name,
    "conv_address": _address,
}


def apply_conversion(value: Any, function: str, *, options: dict[str, Any] | None = None,
                     registry: dict[str, dict[str, Any]] | None = None,
                     reference_data: dict[str, Any] | None = None,
                     null_tokens: Iterable[str] = ()) -> dict[str, Any]:
    """Apply one registered function and return all declared output values."""
    options = dict(options or {})
    registry = registry or {}
    context = ConversionContext(function, options, registry, reference_data or {}, tuple(null_tokens))
    outputs = registered_outputs(function, options.get("column"), registry)
    if _empty(value, context):
        return {output: None for output in outputs} or {options.get("output", options.get("column", "value")): None}
    result = CONVERSIONS[function](value, context) if function in CONVERSIONS else _generic(value, context)
    return {output: result.get(output) for output in outputs} if outputs else result


def registered_outputs(function: str, column: str | None, registry: dict[str, dict[str, Any]]) -> list[str]:
    """Return configured outputs, deriving names from suffixes when available."""
    spec = registry.get(function, {})
    source = column or spec.get("input")
    suffixes = list(spec.get("suffixes", ()))
    if source and suffixes:
        return [str(source) if suffix in ("", None) else f"{source}_{suffix}" for suffix in suffixes]
    return list(spec.get("outputs", ()))