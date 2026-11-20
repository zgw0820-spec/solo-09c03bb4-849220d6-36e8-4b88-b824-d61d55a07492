"""Build OTLP/HTTP JSON payloads for the demo producer in the web UI.

The simulator deliberately produces *two services* so the "entry" batch and
the "downstream" batch look like real, separately reported hops.
"""

from __future__ import annotations

import time
from typing import Any

ENTRY_SERVICE = "checkout-api"
DOWNSTREAM_SERVICE = "inventory-svc"


def _now_ns() -> int:
    return time.time_ns()


def _status(is_error: bool, message: str) -> dict[str, Any]:
    if is_error:
        return {"code": 2, "message": message}
    return {"code": 0}


def _span(
    *,
    trace_id: str,
    span_id: str,
    parent_span_id: str | None,
    name: str,
    kind: int,
    start_ns: int,
    duration_ms: float,
    is_error: bool,
    error_message: str,
    attributes: dict[str, str] | None = None,
) -> dict[str, Any]:
    end_ns = start_ns + int(duration_ms * 1_000_000)
    span: dict[str, Any] = {
        "traceId": trace_id,
        "spanId": span_id,
        "name": name,
        "kind": kind,
        "startTimeUnixNano": str(start_ns),
        "endTimeUnixNano": str(max(end_ns, start_ns)),
        "status": _status(is_error, error_message),
        "attributes": [
            {"key": key, "value": {"stringValue": value}}
            for key, value in (attributes or {}).items()
        ],
    }
    if parent_span_id:
        span["parentSpanId"] = parent_span_id
    return span


def _resource_spans(service: str, spans: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "resource": {
            "attributes": [
                {"key": "service.name", "value": {"stringValue": service}},
            ]
        },
        "scopeSpans": [
            {
                "scope": {"name": "demo-simulator", "version": "1.0.0"},
                "spans": spans,
            }
        ],
    }


def build_otlp_payload(body: dict[str, Any]) -> dict[str, Any]:
    """Turn a simulator request into an OTLP/HTTP JSON trace payload.

    Expected body (all optional)::

        {
          "trace_id": "32 hex chars", "span_id": "16 hex chars",
          "part": "both" | "root" | "downstream",
          "root_duration_ms": 80,
          "root_error": false,
          "downstream_duration_ms": 40,
          "downstream_error": false
        }
    """
    part = str(body.get("part", "both")).lower()
    if part not in {"both", "root", "downstream"}:
        raise ValueError("part must be one of: both, root, downstream")

    trace_id = _hex16(body.get("trace_id"), 32, "trace_id") or _random_hex(32)
    span_id = _hex16(body.get("span_id"), 16, "span_id") or _random_hex(16)
    client_span_id = _random_hex(16)
    server_span_id = _random_hex(16)

    root_duration = _positive_float(body.get("root_duration_ms", 80.0), "root_duration_ms")
    downstream_duration = _positive_float(
        body.get("downstream_duration_ms", 40.0), "downstream_duration_ms"
    )
    root_error = bool(body.get("root_error", False))
    downstream_error = bool(body.get("downstream_error", False))

    root_start = _now_ns() - int(root_duration * 1_000_000)
    # The downstream hop starts a little after the entry span and must fit
    # inside the root span for realistic waterfall diagrams.
    downstream_start = root_start + int(min(root_duration * 0.15, 50.0) * 1_000_000)

    entry_spans: list[dict[str, Any]] = []
    downstream_spans: list[dict[str, Any]] = []
    client_spans: list[dict[str, Any]] = []

    if part in {"both", "root"}:
        entry_spans.append(
            _span(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=None,
                name="POST /checkout",
                kind=2,
                start_ns=root_start,
                duration_ms=root_duration,
                is_error=root_error,
                error_message="checkout failed",
                attributes={"http.method": "POST", "demo.part": "entry"},
            )
        )
    if part in {"both", "downstream"}:
        client_spans.append(
            _span(
                trace_id=trace_id,
                span_id=client_span_id,
                parent_span_id=span_id,
                name="inventory.reserve",
                kind=3,
                start_ns=downstream_start,
                duration_ms=downstream_duration,
                is_error=downstream_error,
                error_message="reserve failed",
                attributes={"demo.part": "downstream"},
            )
        )
        downstream_spans.append(
            _span(
                trace_id=trace_id,
                span_id=server_span_id,
                parent_span_id=client_span_id,
                name="POST /inventory/reserve",
                kind=2,
                start_ns=downstream_start + 1_000_000,
                duration_ms=max(downstream_duration - 1.0, 0.1),
                is_error=downstream_error,
                error_message="reserve failed",
                attributes={"demo.part": "downstream"},
            )
        )

    resource_spans = []
    if entry_spans or client_spans:
        resource_spans.append(_resource_spans(ENTRY_SERVICE, entry_spans + client_spans))
    if downstream_spans:
        resource_spans.append(_resource_spans(DOWNSTREAM_SERVICE, downstream_spans))
    return {"resourceSpans": resource_spans}


def _hex16(value: Any, width: int, field: str) -> str | None:
    if value is None or value == "":
        return None
    text = str(value).strip().lower()
    if len(text) != width:
        raise ValueError(f"{field} must be {width} hex characters")
    try:
        int(text, 16)
    except ValueError as exc:
        raise ValueError(f"{field} must be hexadecimal") from exc
    return text


def _random_hex(width: int) -> str:
    import secrets

    return secrets.token_hex(width // 2)


def _positive_float(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a number") from exc
    if number <= 0:
        raise ValueError(f"{field} must be > 0")
    return number
