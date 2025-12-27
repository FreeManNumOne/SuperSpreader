import argparse
import asyncio
import contextlib

from config.settings import Settings
from monitoring.dashboard import run_dashboard_task
from monitoring.github_publisher import run_github_publisher_task
from storage.sqlite import SqliteStore
from trading.app import run_backtest, run_paper_trader, run_scanner
from utils.logging import configure_logging, get_logger


async def _run() -> None:
    settings = Settings.load()
    configure_logging(settings)
    log = get_logger(__name__)
    store = SqliteStore(settings.sqlite_path)
    store.init_db()

    log.info("app.start", run_mode=settings.run_mode, trade_mode=settings.trade_mode)

    dashboard_task = asyncio.create_task(run_dashboard_task(settings, store))
    publish_task = asyncio.create_task(run_github_publisher_task(settings, store))
    try:
        if settings.run_mode == "scanner":
            await run_scanner(settings, store)
        elif settings.run_mode == "paper":
            await run_paper_trader(settings, store)
        elif settings.run_mode == "backtest":
            await run_backtest(settings, store)
        else:
            raise ValueError(f"Unknown RUN_MODE: {settings.run_mode}")
    finally:
        dashboard_task.cancel()
        publish_task.cancel()
        with contextlib.suppress(Exception):
            await dashboard_task
        with contextlib.suppress(Exception):
            await publish_task


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket trading system")
    parser.add_argument("--mode", choices=["scanner", "paper", "backtest"], help="Override RUN_MODE")
    args = parser.parse_args()

    if args.mode:
        Settings.override_env({"RUN_MODE": args.mode})
    asyncio.run(_run())


if __name__ == "__main__":
    main()
