"""End-to-end tests for the tail-sampling collector.

Time is driven by an injectable clock so the 5 s window can be crossed
instantly; every test goes through the real FastAPI routes and SQLite file.
"""

from __future__ import annotations

import json
import threading

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.store import SqliteStore


class Clock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start
        self.lock = threading.Lock()

    def __call__(self) -> float:
        with self.lock:
            return self.now

    def advance(self, seconds: float) -> None:
        with self.lock:
            self.now += seconds


@pytest.fixture()
def clock() -> Clock:
    return Clock()


def make_settings(tmp_path, clock, **overrides) -> Settings:
    values = dict(
        db_path=str(tmp_path / "collector.db"),
        window_seconds=5.0,
        max_active_traces=8,
        slow_root_ms=500.0,
        sweep_interval_seconds=0.05,
        clock=clock,
    )
    values.update(overrides)
    return Settings(**values)


@pytest.fixture()
def client(tmp_path, clock):
    app = create_app(make_settings(tmp_path, clock))
    with TestClient(app) as test_client:
        yield test_client


def tid(n: int) -> str:
    return f"{n:032x}"


def sid(n: int) -> str:
    return f"{n:016x}"


def span(
    trace_id: str,
    span_id: str,
    *,
    parent: str | None = None,
    name: str = "op",
    duration_ms: float = 10.0,
    error: bool = False,
    service: str = "svc",
) -> dict:
    start = 1_700_000_000_000_000_000
    end = start + int(duration_ms * 1_000_000)
    raw = {
        "traceId": trace_id,
        "spanId": span_id,
        "name": name,
        "kind": 2,
        "startTimeUnixNano": str(start),
        "endTimeUnixNano": str(end),
        "status": {"code": 2 if error else 0},
        "attributes": [{"key": "demo", "value": {"stringValue": "1"}}],
    }
    if parent:
        raw["parentSpanId"] = parent
    return raw


def otlp(*spans: dict, service: str = "svc") -> dict:
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": service}}
                    ]
                },
                "scopeSpans": [
                    {"scope": {"name": "test"}, "spans": list(spans)}
                ],
            }
        ]
    }


def post(client: TestClient, payload: dict):
    response = client.post(
        "/v1/traces", content=json.dumps(payload), headers={"content-type": "application/json"}
    )
    assert response.status_code == 200, response.text
    return response.json()


def trace(client: TestClient, trace_id: str) -> dict:
    response = client.get(f"/api/traces/{trace_id}")
    assert response.status_code == 200, response.text
    return response.json()


# --------------------------------------------------------------------------
# sampling outcomes
# --------------------------------------------------------------------------


def test_error_span_keeps_whole_trace(client, clock):
    post(client, otlp(span(tid(1), sid(1)), service="entry"))
    clock.advance(1)
    post(client, otlp(span(tid(1), sid(2), parent=sid(1), error=True), service="downstream"))
    assert trace(client, tid(1))["state"] == "collecting"

    clock.advance(4.0)  # exactly at deadline
    data = trace(client, tid(1))
    assert data["state"] == "kept"
    assert data["decision_reason"] == "error"
    assert data["retained_spans"] == 2
    assert {s["span_id"] for s in data["spans"]} == {sid(1), sid(2)}


def test_slow_root_keeps_trace(client, clock):
    post(client, otlp(span(tid(2), sid(1), duration_ms=500.001)))
    clock.advance(5.0)
    data = trace(client, tid(2))
    assert data["state"] == "kept"
    assert data["decision_reason"] == "slow_root"
    assert data["retained_spans"] == 1


def test_root_at_threshold_is_not_slow(client, clock):
    post(client, otlp(span(tid(3), sid(1), duration_ms=500.0)))
    clock.advance(5.0)
    assert trace(client, tid(3))["state"] == "dropped"


def test_clean_trace_is_dropped_and_spans_purged(client, clock):
    post(client, otlp(span(tid(4), sid(1)), span(tid(4), sid(2), parent=sid(1))))
    clock.advance(5.0)
    data = trace(client, tid(4))
    assert data["state"] == "dropped"
    assert data["decision_reason"] == "clean"
    assert data["spans"] == []
    assert data["retained_spans"] == 0


def test_error_outside_window_is_too_late(client, clock):
    post(client, otlp(span(tid(5), sid(1))))
    clock.advance(5.0)
    assert trace(client, tid(5))["state"] == "dropped"
    body = post(client, otlp(span(tid(5), sid(2), error=True)))
    assert body["partialSuccess"]["rejectedSpans"] == 1
    assert "window closed" in body["partialSuccess"]["errorMessage"]
    data = trace(client, tid(5))
    assert data["state"] == "dropped"  # late span cannot change the decision
    assert data["spans"] == []


def test_late_span_after_keep_is_also_rejected(client, clock):
    post(client, otlp(span(tid(6), sid(1), error=True)))
    clock.advance(5.0)
    assert trace(client, tid(6))["state"] == "kept"
    body = post(client, otlp(span(tid(6), sid(2))))
    assert body["partialSuccess"]["rejectedSpans"] == 1
    assert trace(client, tid(6))["retained_spans"] == 1


def test_duplicate_span_counted_once(client, clock):
    payload = otlp(span(tid(7), sid(1)), span(tid(7), sid(2), parent=sid(1)))
    post(client, payload)
    body = post(client, payload)
    assert body.get("partialSuccess") is None
    data = trace(client, tid(7))
    assert data["span_count"] == 2
    assert data["retained_spans"] == 2
    # An identical span with different content still is the same span id.
    post(client, otlp(span(tid(7), sid(2), name="renamed")))
    assert trace(client, tid(7))["span_count"] == 2


def test_root_arriving_later_still_sets_decision(client, clock):
    """Root may be reported after its children; its duration still decides."""
    post(client, otlp(span(tid(8), sid(2), parent=sid(1))))
    clock.advance(2)
    post(client, otlp(span(tid(8), sid(1), duration_ms=900)))
    clock.advance(3.0)
    data = trace(client, tid(8))
    assert data["state"] == "kept"
    assert data["decision_reason"] == "slow_root"


# --------------------------------------------------------------------------
# capacity
# --------------------------------------------------------------------------


def test_capacity_eight_rejects_new_trace_but_accepts_existing(client, clock):
    for n in range(1, 9):
        post(client, otlp(span(tid(100 + n), sid(1))))
    assert client.get("/api/stats").json()["counts"]["collecting"] == 8

    rejected = post(client, otlp(span(tid(999), sid(1))))
    assert rejected["partialSuccess"]["rejectedSpans"] == 1
    assert "capacity full" in rejected["partialSuccess"]["errorMessage"]
    assert client.get(f"/api/traces/{tid(999)}").status_code == 404

    # Already-collecting traces can still receive spans.
    accepted = post(
        client, otlp(span(tid(101), sid(2), parent=sid(1), error=True))
    )
    assert "partialSuccess" not in accepted
    assert trace(client, tid(101))["retained_spans"] == 2

    # Same batch: existing trace accepted, brand-new trace rejected.
    mixed = post(
        client,
        otlp(
            span(tid(102), sid(2), parent=sid(1)),
            span(tid(998), sid(1)),
        ),
    )
    assert mixed["partialSuccess"]["rejectedSpans"] == 1
    assert trace(client, tid(102))["retained_spans"] == 2


def test_decision_frees_slot_immediately(client, clock):
    for n in range(1, 9):
        post(client, otlp(span(tid(200 + n), sid(1), error=True)))
    assert post(client, otlp(span(tid(300), sid(1))))["partialSuccess"]["rejectedSpans"] == 1

    clock.advance(5.0)
    # First request after expiry: decision is applied and the slot reused in
    # the same request.
    body = post(client, otlp(span(tid(300), sid(1))))
    assert "partialSuccess" not in body
    assert trace(client, tid(201))["state"] == "kept"
    assert trace(client, tid(300))["state"] == "collecting"


def test_background_sweep_releases_capacity_without_traffic(client, clock):
    for n in range(1, 9):
        post(client, otlp(span(tid(400 + n), sid(1), error=True)))
    clock.advance(5.0)
    # The list endpoint settles, mirroring what the sweeper does continuously.
    listing = client.get("/api/traces").json()
    assert listing["counts"]["collecting"] == 0
    assert listing["counts"]["kept"] == 8
    assert "partialSuccess" not in post(client, otlp(span(tid(500), sid(1))))


# --------------------------------------------------------------------------
# persistence and protocol
# --------------------------------------------------------------------------


def test_deadline_survives_restart(tmp_path, clock):
    settings = make_settings(tmp_path, clock)
    app1 = create_app(settings)
    with TestClient(app1) as c1:
        post(c1, otlp(span(tid(600), sid(1), error=True)))
        clock.advance(2)
        first = trace(c1, tid(600))
        assert first["state"] == "collecting"
        original_deadline = first["deadline_at"]

    # Simulate a restart 2 s later with a fresh app instance on the same DB.
    clock.advance(2)
    app2 = create_app(make_settings(tmp_path, clock))
    with TestClient(app2) as c2:
        resumed = trace(c2, tid(600))
        assert resumed["state"] == "collecting"
        assert resumed["deadline_at"] == original_deadline
        assert resumed["remaining_seconds"] == pytest.approx(1.0)

        clock.advance(1.0)  # original deadline, not "restart + 5 s"
        after = trace(c2, tid(600))
        assert after["state"] == "kept"
        assert after["retained_spans"] == 1


def test_partial_success_shape_and_invalid_span(client):
    body = post(
        client,
        otlp(
            span(tid(700), sid(1), error=True),
            {"traceId": "not-hex", "spanId": sid(9), "name": "bad"},
        ),
    )
    assert body["partialSuccess"]["rejectedSpans"] == 1
    assert "invalid" in body["partialSuccess"]["errorMessage"]
    assert trace(client, tid(700))["state"] == "collecting"


def test_non_json_and_bad_body_rejected(client):
    response = client.post(
        "/v1/traces", content=b"\x00\x01", headers={"content-type": "application/x-protobuf"}
    )
    assert response.status_code == 400
    response = client.post(
        "/v1/traces", content=b"{not json", headers={"content-type": "application/json"}
    )
    assert response.status_code == 400
    response = client.post(
        "/v1/traces", content=b"[]", headers={"content-type": "application/json"}
    )
    assert response.status_code == 400


def test_simulator_two_batch_flow(client, clock):
    first = client.post(
        "/api/simulate",
        json={"part": "root", "root_duration_ms": 30, "root_error": False},
    ).json()
    trace_id = first["trace_ids"][0]
    assert first["accepted"] == 1

    clock.advance(1)
    second = client.post(
        "/api/simulate",
        json={"part": "downstream", "trace_id": trace_id, "downstream_error": True},
    ).json()
    assert second["accepted"] == 2  # downstream client + server spans

    clock.advance(4.0)
    data = trace(client, trace_id)
    assert data["state"] == "kept"
    assert data["decision_reason"] == "error"
    assert data["retained_spans"] == 3
    assert data["service"] == "checkout-api"


def test_simulate_uses_real_otlp_path(client):
    body = client.post("/api/simulate", json={"root_duration_ms": 600}).json()
    trace_id = body["trace_ids"][0]
    assert trace(client, trace_id)["state"] == "collecting"
    assert trace(client, trace_id)["root_duration_ns"] == 600_000_000


def test_ui_serves(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "OTLP" in response.text
    assert client.get("/healthz").json()["status"] == "ok"


def test_store_is_not_shared_between_writes(client, clock):
    """pytest sanity: each test gets a fresh DB, so ids never leak."""
    assert client.get("/api/traces").json()["traces"] == []


def test_sqlite_store_direct_expiry(tmp_path, clock):
    settings = make_settings(tmp_path, clock)
    store = SqliteStore(settings)
    store.init_schema()
    from app.otlp import parse_otlp_json

    parsed = parse_otlp_json(otlp(span(tid(800), sid(1))))
    store.ingest(parsed.spans, clock())
    assert store.settle_expired(clock()) == []
    clock.advance(5.0)
    assert store.settle_expired(clock()) == [tid(800)]
    assert store.settle_expired(clock()) == []  # idempotent
