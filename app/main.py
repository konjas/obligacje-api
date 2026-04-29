"""
API do obsługi notowań detalicznych obligacji skarbowych.

Endpoints:
  GET  /prices/{ticker}?purchase_date=YYYY-MM-DD  – wycena dla daty zakupu
  GET  /status                                     – stan bazy danych
  GET  /bonds                                       – lista śledzonych obligacji
  POST /refresh                                    – ręczne uruchomienie scrapera
  POST /refresh/{ticker}                           – ręczne odświeżenie jednego tickera
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from .config import load_config
from .database import Database
from .scheduler import create_scheduler
from .scraper import is_matured, parse_ticker_maturity, scrape_all_tickers, scrape_ticker

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Globals ──────────────────────────────────────────────────────────────────

CONFIG_PATH = os.getenv("CONFIG_PATH", "/config/config.yaml")
config = load_config(CONFIG_PATH)
db = Database(os.path.join(config.data_dir, "bonds.db"))

_scraping_lock = asyncio.Lock()
_scraping_in_progress = False


# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Startuje aplikację. Śledzono tickery: {config.tickers}")
    scheduler = create_scheduler(config, db)
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(
    title="Obligacje Skarbowe – Notowania",
    description="API do wyceny detalicznych obligacji skarbowych na podstawie tabel odsetkowych MF",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _calculate_prices(periods_data: list, purchase_date: date) -> list:
    """
    Oblicza dzienne wyceny dla podanej daty zakupu.

    Logika:
    - Każdy okres startuje od 0 odsetek.
    - Na koniec okresu odsetki są kapitalizowane (dodawane do bazy następnego okresu).
    - Wzór: cena = baza_okresu + odsetki_bieżącego_okresu

    Zwraca listę słowników: {date, price, interest, period, days_held}
    """
    result = []
    base = 100.0  # PLN – nominalna wartość 1 sztuki
    seen_dates: set = set()
    running_day = 0

    for period in periods_data:
        entries = period["entries"]  # [(day_offset, interest)]

        for day_offset, interest in entries:
            total_days = running_day + day_offset
            cal_date = purchase_date + timedelta(days=total_days)
            price = round(base + interest, 4)

            if cal_date not in seen_dates:
                seen_dates.add(cal_date)
                result.append(
                    {
                        "date": cal_date.isoformat(),
                        "price": price,
                        "interest": round(interest, 4),
                        "period": period["period_index"],
                        "days_held": total_days,
                    }
                )

        # Kapitalizacja: ostatnie odsetki okresu wchodzą do bazy następnego
        running_day += len(entries)

    return result


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get(
    "/prices/{ticker}",
    summary="Wycena obligacji dla podanej daty zakupu",
    response_class=JSONResponse,
)
def get_prices(
    ticker: str,
    purchase_date: str = Query(..., description="Data zakupu w formacie YYYY-MM-DD"),
    date_from: Optional[str] = Query(None, description="Opcjonalnie: ogranicz wyniki od tej daty"),
    date_to: Optional[str] = Query(None, description="Opcjonalnie: ogranicz wyniki do tej daty"),
):
    ticker = ticker.upper().strip()

    if ticker not in config.tickers:
        raise HTTPException(
            status_code=404,
            detail=f"Ticker '{ticker}' nie jest śledzony. Dostępne: {config.tickers}",
        )

    # Parsowanie dat
    try:
        pd = date.fromisoformat(purchase_date)
    except ValueError:
        raise HTTPException(status_code=422, detail="Nieprawidłowy format purchase_date (oczekiwany: YYYY-MM-DD)")

    try:
        df = date.fromisoformat(date_from) if date_from else None
        dt = date.fromisoformat(date_to) if date_to else None
    except ValueError:
        raise HTTPException(status_code=422, detail="Nieprawidłowy format date_from lub date_to")

    periods_data = db.get_periods_with_entries(ticker)
    if not periods_data:
        raise HTTPException(
            status_code=404,
            detail=f"Brak danych dla tickera '{ticker}'. Uruchom /refresh aby pobrać dane.",
        )

    prices = _calculate_prices(periods_data, pd)

    # Filtrowanie opcjonalne
    if df:
        prices = [p for p in prices if p["date"] >= df.isoformat()]
    if dt:
        prices = [p for p in prices if p["date"] <= dt.isoformat()]

    maturity = parse_ticker_maturity(ticker)
    # maturity_date: ten sam dzień miesiąca co purchase_date, ale w miesiącu wykupu.
    # Np. zakup 15-03-2026, wykup marzec 2036 → maturity_date = 15-03-2036.
    maturity_date_str = None
    if maturity:
        import calendar as _cal
        last_day = _cal.monthrange(maturity.year, maturity.month)[1]
        day = min(pd.day, last_day)
        maturity_date_str = date(maturity.year, maturity.month, day).isoformat()

    return {
        "ticker": ticker,
        "purchase_date": purchase_date,
        "maturity_date": maturity_date_str,
        "periods_available": len(periods_data),
        "data_points": len(prices),
        "prices": prices,
    }


@app.get("/status", summary="Stan bazy danych i trackera")
def get_status():
    stats = db.get_status()
    bonds = db.list_bonds()

    tracked = []
    for ticker in config.tickers:
        maturity = parse_ticker_maturity(ticker)
        bond_info = db.get_bond(ticker) or {}
        periods = db.get_periods_with_entries(ticker)
        tracked.append(
            {
                "ticker": ticker,
                "maturity_date": str(maturity) if maturity else None,
                "is_matured": is_matured(ticker),
                "periods_downloaded": len(periods),
                "in_db": bool(bond_info),
            }
        )

    return {
        "configured_tickers": config.tickers,
        "tracked": tracked,
        "db_stats": stats,
        "scraping_in_progress": _scraping_in_progress,
    }


@app.get("/bonds", summary="Lista śledzonych obligacji")
def list_bonds():
    result = []
    for ticker in config.tickers:
        maturity = parse_ticker_maturity(ticker)
        periods = db.get_periods_with_entries(ticker)
        result.append(
            {
                "ticker": ticker,
                "maturity_date": str(maturity) if maturity else None,
                "is_matured": is_matured(ticker),
                "periods": [
                    {
                        "index": p["period_index"],
                        "label": p["period_label"],
                        "period_start": p["period_start_date"],
                        "rate_pct": p["rate_pct"],
                        "entries": len(p["entries"]),
                    }
                    for p in periods
                ],
            }
        )
    return {"bonds": result}


@app.post("/refresh", summary="Ręczne uruchomienie scrapera dla wszystkich tickerów")
async def refresh_all(background_tasks: BackgroundTasks):
    global _scraping_in_progress
    if _scraping_in_progress:
        raise HTTPException(status_code=409, detail="Scraping już jest w toku")

    background_tasks.add_task(_run_scraping_all)
    return {"message": "Scraping uruchomiony w tle dla wszystkich tickerów", "tickers": config.tickers}


@app.post("/refresh/{ticker}", summary="Ręczne odświeżenie jednego tickera")
async def refresh_ticker(ticker: str, background_tasks: BackgroundTasks):
    global _scraping_in_progress
    ticker = ticker.upper().strip()

    if ticker not in config.tickers:
        raise HTTPException(
            status_code=404,
            detail=f"Ticker '{ticker}' nie jest śledzony. Dostępne: {config.tickers}",
        )
    if is_matured(ticker):
        raise HTTPException(status_code=400, detail=f"Ticker '{ticker}' już wykupiony")
    if _scraping_in_progress:
        raise HTTPException(status_code=409, detail="Scraping już jest w toku")

    background_tasks.add_task(_run_scraping_one, ticker)
    return {"message": f"Scraping uruchomiony w tle dla {ticker}"}


# ── Background tasks ─────────────────────────────────────────────────────────

async def _run_scraping_all():
    global _scraping_in_progress
    async with _scraping_lock:
        _scraping_in_progress = True
        try:
            await scrape_all_tickers(config, db)
        except Exception as e:
            logger.error(f"Błąd podczas scrapowania: {e}", exc_info=True)
        finally:
            _scraping_in_progress = False


async def _run_scraping_one(ticker: str):
    global _scraping_in_progress
    async with _scraping_lock:
        _scraping_in_progress = True
        try:
            from playwright.async_api import async_playwright

            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=config.headless,
                    args=["--no-sandbox", "--disable-setuid-sandbox"],
                )
                context = await browser.new_context(accept_downloads=True)
                try:
                    await scrape_ticker(
                        ticker, context, db, Path(config.data_dir), config.scraper_timeout
                    )
                finally:
                    await browser.close()
        except Exception as e:
            logger.error(f"Błąd scrapowania {ticker}: {e}", exc_info=True)
        finally:
            _scraping_in_progress = False
