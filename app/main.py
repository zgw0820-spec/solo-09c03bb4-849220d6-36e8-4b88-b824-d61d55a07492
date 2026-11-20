"""FastAPI application: OTLP/HTTP JSON intake plus a small inspection UI."""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import __version__
from .config import Settings
from .otlp import PayloadError, parse_otlp_json
from .store import SqliteStore

logger = logging.getLogger("collector")

STATIC_DIR = Path(__file__).parent / "static"
CONTENT_TYPE_JSON = "application/json"


def _otlp_response(partial: dict[str, Any] | None) -> Response:
    body: dict[str, Any] = {}
    if partial:
        body["partialSuccess"] = partial
    return JSONResponse(body, status_code=200, media_type=CONTENT_TYPE_JSON)


async def _read_json(request: Request) -> Any:
    content_type = request.headers.get("content-type", "")
    if "json" not in content_type.lower():
        raise PayloadError(
            "unsupported content type; this receiver accepts OTLP/HTTP JSON "
            "(Content-Type: application/json)"
        )
    raw = await request.body()
    if request.headers.get("content-encoding", "").lower() == "gzip":
        try:
            raw = gzip.decompress(raw)
        except OSError as exc:
            raise PayloadError(f"invalid gzip body: {exc}") from exc
    if not raw:
        raise PayloadError("empty request body")
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise PayloadError(f"invalid JSON: {exc}") from exc


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    store = SqliteStore(settings)
    store.init_schema()

    async def sweep_loop() -> None:
        interval = settings.sweep_interval_seconds
        while True:
            await asyncio.sleep(interval)
            try:
                await asyncio.to_thread(store.settle_expired, settings.clock())
            except Exception:  # pragma: no cover - defensive background logging
                logger.exception("background sweep failed")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = asyncio.create_task(sweep_loop())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    app = FastAPI(
        title="OTLP Tail Sampling Collector",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.store = store

    @app.exception_handler(PayloadError)
    async def payload_error_handler(request: Request, exc: PayloadError) -> Response:
        del request
        return JSONResponse({"error": str(exc)}, status_code=400)

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"status": "ok", "version": __version__}

    @app.get("/api/config")
    async def api_config() -> dict[str, Any]:
        return {
            "version": __version__,
            "window_seconds": settings.window_seconds,
            "max_active_traces": settings.max_active_traces,
            "slow_root_ms": settings.slow_root_ms,
            "sweep_interval_seconds": settings.sweep_interval_seconds,
        }

    # -- OTLP/HTTP ingestion ------------------------------------------------

    @app.post("/v1/traces")
    async def ingest_traces(request: Request) -> Response:
        payload = await _read_json(request)
        parsed = parse_otlp_json(payload)
        result = await asyncio.to_thread(
            store.ingest, parsed.spans, settings.clock(), parsed.invalid
        )
        partial = None
        if result.rejected:
            partial = {"rejectedSpans": result.rejected, "errorMessage": result.rejection_message()}
        return _otlp_response(partial)

    # -- inspection API -----------------------------------------------------

    @app.get("/api/traces")
    async def api_list_traces(
        limit: int = Query(50, ge=1, le=500),
        state: str | None = Query(None, pattern="^(collecting|kept|dropped)$"),
    ) -> dict[str, Any]:
        return store.list_traces(settings.clock(), limit=limit, state=state)

    @app.get("/api/traces/{trace_id}")
    async def api_trace(trace_id: str) -> Response:
        trace = store.settle_and_get(trace_id.lower(), settings.clock())
        if trace is None:
            return JSONResponse({"error": "trace not found"}, status_code=404)
        return JSONResponse(trace)

    @app.get("/api/stats")
    async def api_stats() -> dict[str, Any]:
        data = store.list_traces(settings.clock(), limit=1)
        return {
            "counts": data["counts"],
            "capacity": data["capacity"],
            "now": data["now"],
        }

    # -- simulated producer (batch reporting entry + downstream) ------------

    @app.post("/api/simulate")
    async def api_simulate(body: dict[str, Any]) -> dict[str, Any]:
        """Build OTLP JSON spans and feed them through the real intake path.

        ``part`` selects which batch is sent: ``"both"`` (default) reports the
        entry/server span and the downstream client+server spans in one
        request, ``"root"`` or ``"downstream"`` send them as separate batches
        to emulate staggered downstream reporting.
        """
        from .simulator import build_otlp_payload

        part = body.get("part", "both")
        try:
            payload = build_otlp_payload(body)
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        parsed = parse_otlp_json(payload)
        result = await asyncio.to_thread(
            store.ingest, parsed.spans, settings.clock(), parsed.invalid
        )
        return {
            "part": part,
            "submitted_spans": len(parsed.spans),
            "accepted": result.accepted,
            "duplicates": result.duplicates,
            "rejected": result.rejected,
            "rejected_capacity": result.rejected_capacity,
            "rejected_late": result.rejected_late,
            "trace_ids": sorted({span.trace_id for span in parsed.spans}),
            "root_span_id": next(
                (span.span_id for span in parsed.spans if span.is_root), None
            ),
            "admitted_traces": result.admitted_traces,
        }

    if STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

        @app.get("/", response_class=HTMLResponse)
        async def index() -> HTMLResponse:
            return HTMLResponse((STATIC_DIR / "index.html").read_text(encoding="utf-8"))

    return app


app = create_app()
