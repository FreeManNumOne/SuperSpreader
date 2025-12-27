from __future__ import annotations

import asyncio
import contextlib
import webbrowser
from typing import Any

import uvicorn

from monitoring.web_dashboard import build_app
from utils.logging import get_logger


async def run_dashboard_task(settings: Any, store: Any) -> None:
    """
    Local web dashboard (optional). Safe to run alongside any mode.
    """
    log = get_logger(__name__)
    if not bool(getattr(settings, "dashboard_enabled", True)):
        while True:
            await asyncio.sleep(3600)

    host = str(getattr(settings, "dashboard_host", "127.0.0.1"))
    port = int(getattr(settings, "dashboard_port", 8000))
    url = f"http://{host}:{port}/"

    app = build_app(settings, store)
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level="warning",
        access_log=False,
        lifespan="on",
    )
    server = uvicorn.Server(config)

    async def _maybe_open_browser() -> None:
        if not bool(getattr(settings, "dashboard_open_browser", True)):
            return
        # Give the server a moment to bind before opening.
        await asyncio.sleep(0.6)
        await asyncio.to_thread(webbrowser.open, url, new=2, autoraise=True)

    open_task = asyncio.create_task(_maybe_open_browser())
    try:
        log.info("dashboard.start", url=url)
        await server.serve()
    except asyncio.CancelledError:
        server.should_exit = True
        raise
    except Exception:
        log.exception("dashboard.error")
    finally:
        open_task.cancel()
        with contextlib.suppress(Exception):
            await open_task

