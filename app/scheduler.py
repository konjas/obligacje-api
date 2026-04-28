import asyncio
import logging
from datetime import date

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import AppConfig
from .database import Database

logger = logging.getLogger(__name__)


def create_scheduler(config: AppConfig, db: Database) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")

    scheduler.add_job(
        _monthly_refresh,
        CronTrigger(day=config.check_day, hour=config.check_hour, minute=0),
        args=[config, db],
        id="monthly_refresh",
        name="Miesięczne pobieranie nowych tabel odsetkowych",
        replace_existing=True,
    )

    logger.info(
        f"Scheduler skonfigurowany: dzień {config.check_day} każdego miesiąca o {config.check_hour}:00 UTC"
    )
    return scheduler


async def _monthly_refresh(config: AppConfig, db: Database):
    """Pobiera nowe dane dla tickerów, które nie zostały jeszcze wykupione."""
    from .scraper import is_matured, scrape_all_tickers

    logger.info("=== Automatyczne miesięczne odświeżanie danych ===")

    active = [t for t in config.tickers if not is_matured(t)]
    matured = [t for t in config.tickers if is_matured(t)]

    if matured:
        logger.info(f"Wykupione tickery (pomijam): {matured}")
    if not active:
        logger.info("Brak aktywnych tickerów do odświeżenia")
        return

    logger.info(f"Aktywne tickery do sprawdzenia: {active}")
    try:
        await scrape_all_tickers(config, db)
    except Exception as e:
        logger.error(f"Błąd podczas automatycznego odświeżania: {e}", exc_info=True)

    logger.info("=== Zakończono automatyczne odświeżanie ===")
