"""Minimal, tolerant OTLP/HTTP JSON decoding.

Only the pieces needed for tail sampling are extracted from each span; the raw
span object is kept so the UI can show exactly what was received.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from typing import Any, Iterable

ERROR_STATUS = 2
STATUS_ERROR_NAMES = {"2", "STATUS_CODE_ERROR", "ERROR"}


class PayloadError(ValueError):
    """The request body is not a valid OTLP JSON trace payload."""


@dataclass(frozen=True)
class ParsedSpan:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    start_ns: int | None
    end_ns: int | None
    duration_ns: int | None
    is_root: bool
    is_error: bool
    service: str | None
    scope: str | None
    body: str


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return None


def parse_int64(value: Any) -> int | None:
    """OTLP JSON encodes int64 as a decimal string, but be liberal."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text, 10)
        except ValueError:
            return None
    return None


def normalize_id(value: Any, width: int) -> str | None:
    """Return a lower-case hex OTLP id, or ``None`` if invalid/empty.

    OTLP JSON specifies hex strings for ``traceId``/``spanId``.  Producers that
    use protobuf's generic JSON mapping emit base64 instead, so that form is
    accepted too and converted to the canonical hex representation.
    """
    text = _as_text(value)
    if text is None:
        return None
    text = text.strip()
    if not text or set(text) == {"0"}:
        return None

    if len(text) == width:
        try:
            int(text, 16)
        except ValueError:
            pass
        else:
            return text.lower()

    # Base64-encoded bytes (standard protobuf JSON mapping).
    try:
        raw = base64.b64decode(text, validate=True)
    except (binascii.Error, ValueError):
        return None
    if len(raw) != width // 2:
        return None
    hexed = raw.hex()
    if set(hexed) == {"0"}:
        return None
    return hexed


def is_error_status(status: Any) -> bool:
    if not isinstance(status, dict):
        return False
    code = status.get("code")
    if isinstance(code, bool):
        return False
    if isinstance(code, int):
        return code == ERROR_STATUS
    if isinstance(code, str):
        return code.strip().upper() in STATUS_ERROR_NAMES
    return False


def _attributes_to_service(attributes: Any) -> str | None:
    if not isinstance(attributes, list):
        return None
    for attr in attributes:
        if not isinstance(attr, dict):
            continue
        if attr.get("key") != "service.name":
            continue
        value = attr.get("value")
        if isinstance(value, dict):
            return _as_text(value.get("stringValue"))
        return _as_text(value)
    return None


def _iter_spans(payload: Any) -> Iterable[tuple[dict, str | None, str | None]]:
    """Yield ``(span, service_name, scope_name)`` from an OTLP JSON payload."""
    if not isinstance(payload, dict):
        raise PayloadError("payload must be a JSON object")
    resource_spans = payload.get("resourceSpans", [])
    if resource_spans is None:
        resource_spans = []
    if not isinstance(resource_spans, list):
        raise PayloadError("resourceSpans must be a list")
    for resource_span in resource_spans:
        if not isinstance(resource_span, dict):
            raise PayloadError("resourceSpans entries must be objects")
        resource = resource_span.get("resource")
        service = None
        if isinstance(resource, dict):
            service = _attributes_to_service(resource.get("attributes"))
        # Current OTLP JSON name is "scopeSpans"; older exporters use
        # "instrumentationLibrarySpans" and "instrumentationLibrary".
        scope_spans = resource_span.get("scopeSpans")
        if scope_spans is None:
            scope_spans = resource_span.get("instrumentationLibrarySpans", [])
        if scope_spans is None:
            scope_spans = []
        if not isinstance(scope_spans, list):
            raise PayloadError("scopeSpans must be a list")
        for scope_span in scope_spans:
            if not isinstance(scope_span, dict):
                raise PayloadError("scopeSpans entries must be objects")
            scope = scope_span.get("scope")
            if scope is None:
                scope = scope_span.get("instrumentationLibrary")
            scope_name = _as_text(scope.get("name")) if isinstance(scope, dict) else None
            spans = scope_span.get("spans", [])
            if spans is None:
                spans = []
            if not isinstance(spans, list):
                raise PayloadError("spans must be a list")
            for span in spans:
                if not isinstance(span, dict):
                    raise PayloadError("spans entries must be objects")
                yield span, service, scope_name


@dataclass(frozen=True)
class ParseResult:
    spans: tuple[ParsedSpan, ...]
    invalid: int = 0


def parse_otlp_json(payload: Any) -> ParseResult:
    """Decode an OTLP JSON payload into :class:`ParsedSpan` objects.

    Spans with unusable ids are skipped and counted as ``invalid`` so the rest
    of the batch is still processed; structural errors raise
    :class:`PayloadError`.
    """
    parsed: list[ParsedSpan] = []
    invalid = 0
    for span, service, scope in _iter_spans(payload):
        trace_id = normalize_id(span.get("traceId"), 32)
        span_id = normalize_id(span.get("spanId"), 16)
        if trace_id is None or span_id is None:
            invalid += 1
            continue
        parent_span_id = normalize_id(span.get("parentSpanId"), 16)
        start_ns = parse_int64(span.get("startTimeUnixNano"))
        end_ns = parse_int64(span.get("endTimeUnixNano"))
        duration_ns = None
        if start_ns is not None and end_ns is not None and end_ns >= start_ns:
            duration_ns = end_ns - start_ns
        parsed.append(
            ParsedSpan(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=parent_span_id,
                name=_as_text(span.get("name")) or "",
                start_ns=start_ns,
                end_ns=end_ns,
                duration_ns=duration_ns,
                is_root=parent_span_id is None,
                is_error=is_error_status(span.get("status")),
                service=service,
                scope=scope,
                body=json.dumps(span, separators=(",", ":"), sort_keys=True),
            )
        )
    return ParseResult(tuple(parsed), invalid)
