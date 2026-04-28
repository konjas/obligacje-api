"""
Scraper oparty na Playwright do pobierania tabel odsetkowych
ze strony https://www.obligacjeskarbowe.pl/tabela-odsetkowa/

Strona używa choices.js. Interakcja:
  - produkt: zmiana przez URL (?product=edo) lub kliknięcie choices
  - emisja/okres: kliknięcie choices__inner, potem opcji po roli/nazwie
  - widoczne okresy (dla wybranej emisji): brak klasy choices__item--hidden
"""
import logging
import re
from datetime import date
from pathlib import Path
from typing import List, Optional, Tuple

from playwright.async_api import BrowserContext, Page, async_playwright

from .config import AppConfig
from .database import Database
from .parser import parse_interest_pdf

logger = logging.getLogger(__name__)

WEBSITE_URL = "https://www.obligacjeskarbowe.pl/tabela-odsetkowa/"

TICKER_PREFIX_TO_VALUE = {
    "POS": "pos", "ROR": "ror", "DOS": "dos", "DOR": "dor",
    "TOZ": "toz", "TOS": "tos", "COI": "coi", "ROS": "ros",
    "EDO": "edo", "ROD": "rod", "KOS": "kos",
}


# ── Parsowanie tickera ───────────────────────────────────────────────────────

def parse_ticker_maturity(ticker: str) -> Optional[date]:
    """Format MMYY: EDO0336 → MM=03, YY=36 → marzec 2036"""
    if len(ticker) < 7:
        return None
    suffix = ticker[3:7]
    try:
        mm, yy = int(suffix[:2]), int(suffix[2:])
        if 1 <= mm <= 12 and 20 <= yy <= 99:
            return date(2000 + yy, mm, 1)
    except ValueError:
        pass
    logger.warning(f"Nie udało się rozpoznać daty wykupu tickera: {ticker}")
    return None


def is_matured(ticker: str) -> bool:
    maturity = parse_ticker_maturity(ticker)
    if maturity is None:
        return False
    return date.today() >= maturity


# ── Główna funkcja scrapera ──────────────────────────────────────────────────

async def scrape_all_tickers(config: AppConfig, db: Database):
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=config.headless,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        context = await browser.new_context(accept_downloads=True)
        await context.tracing.start(screenshots=True, snapshots=True) # debug start

        for ticker in config.tickers:
            if is_matured(ticker):
                logger.info(f"Ticker {ticker} wykupiony – pomijam")
                continue
            try:
                await scrape_ticker(ticker, context, db, Path(config.data_dir), config.scraper_timeout)
            except Exception as e:
                logger.error(f"Błąd scrapowania {ticker}: {e}", exc_info=True)

        await context.tracing.stop(path="/data/trace.zip") # debug stop
        await browser.close()


async def scrape_ticker(
    ticker: str,
    context: BrowserContext,
    db: Database,
    data_dir: Path,
    timeout: int = 45000,
):
    logger.info(f"Scrapuję ticker: {ticker}")

    prefix = ticker[:3].upper()
    product_value = TICKER_PREFIX_TO_VALUE.get(prefix)
    if not product_value:
        logger.error(f"Nieznany prefix tickera: {prefix}")
        return

    page = await context.new_page()
    try:
        # 1. Załaduj stronę z produktem przez URL
        url = f"{WEBSITE_URL}?product={product_value}"
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        #await page.wait_for_load_state("networkidle", timeout=timeout)

        # 2. Akceptuj cookies jeśli pojawi się banner
        await _accept_cookies(page)

        # 3. Wybierz emisję (ticker) przez kliknięcie choices dropdown
        product_label = await _get_product_label(page)
        logger.info(f"Produkt: {product_label}")

        emission_ok = await _select_emission(page, ticker, timeout)
        if not emission_ok:
            logger.error(f"Nie znaleziono emisji {ticker}")
            return

        # 4. Pobierz widoczne okresy (bez klasy --hidden) dla wybranej emisji
        periods = await _get_visible_periods(page)
        if not periods:
            logger.warning(f"Brak dostępnych okresów dla {ticker}")
            return
        logger.info(f"Znaleziono {len(periods)} okres(ów) dla {ticker}")

        existing = set(db.get_existing_period_labels(ticker))
        maturity = parse_ticker_maturity(ticker)
        db.upsert_bond(ticker, product_label or product_value, str(maturity) if maturity else None)

        # 5. Dla każdego nowego okresu: wybierz, pobierz PDF, parsuj
        for period_idx, period in enumerate(periods, start=1):
            label = period["text"]
            if label in existing:
                logger.info(f"  Okres '{label}' już pobrany – pomijam")
                continue

            logger.info(f"  Pobieram okres {period_idx}/{len(periods)}: '{label}'")
            pdf_path = await _download_period_pdf(
                page, period, period_idx, ticker, data_dir, timeout
            )
            if not pdf_path:
                continue

            parsed = parse_interest_pdf(str(pdf_path))
            if not parsed or not parsed.get("entries"):
                logger.warning(f"  Nie udało się sparsować {pdf_path}")
                continue

            db.save_period_with_entries(
                ticker=ticker,
                period_index=period_idx,
                period_label=label,
                period_start_date=parsed.get("period_start"),
                rate_pct=parsed.get("rate_pct"),
                pdf_path=str(pdf_path),
                entries=parsed["entries"],
            )
            logger.info(f"  Zapisano {len(parsed['entries'])} wpisów dla okresu '{label}'")

    finally:
        await page.close()


# ── Helpers ──────────────────────────────────────────────────────────────────

async def _accept_cookies(page: Page):
    """Klikamy baner cookies jeśli jest widoczny."""
    try:
        btn = page.get_by_role("link", name="Akceptuję")
        if await btn.count() > 0:
            await btn.first.click()
            await page.wait_for_timeout(500)
            logger.debug("Zaakceptowano cookies")
    except Exception:
        pass  # brak banera – OK


async def _get_product_label(page: Page) -> Optional[str]:
    """Zwraca aktualnie wybrany produkt (tekst w choices__inner)."""
    try:
        el = page.locator(
            "#id_type_bonds ~ .choices__list--single .choices__item"
        ).first
        # alternatywna ścieżka przez choices__inner
        el2 = page.locator(
            ".filter-block__item:has(#id_type_bonds) .choices__inner .choices__item"
        ).first
        if await el2.count() > 0:
            return (await el2.inner_text()).strip()
        if await el.count() > 0:
            return (await el.inner_text()).strip()
    except Exception:
        pass
    return None


async def _select_emission(page: Page, ticker: str, timeout: int) -> bool:
    """
    Klika w dropdown emisji i wybiera opcję pasującą do tickera.
    Używa wzorca wygenerowanego przez codegen:
      1. klik na .choices__inner żeby otworzyć dropdown
      2. klik na opcję przez get_by_role("option", name=ticker)
    """
    try:
        # Otwórz dropdown emisji
        inner = page.locator(
            ".filter-block__item.wrap-id_issue_bonds > .choices > .choices__inner"
        )
        await inner.click(timeout=timeout)
        await page.wait_for_timeout(300)

        # Kliknij opcję pasującą do tickera
        option = page.get_by_role("option", name=re.compile(ticker, re.IGNORECASE))
        if await option.count() == 0:
            logger.error(f"Opcja '{ticker}' nie znaleziona w dropdownie emisji")
            return False
        await option.first.click(timeout=timeout)
        await page.wait_for_timeout(500)
        logger.debug(f"Wybrano emisję: {ticker}")
        return True
    except Exception as e:
        logger.error(f"_select_emission błąd: {e}")
        return False


async def _get_visible_periods(page: Page) -> List[dict]:
    """
    Zwraca widoczne okresy z dropdownu okresów.
    Widoczne = opcje BEZ klasy choices__item--hidden.
    """
    try:
        # Widoczne opcje to te bez --hidden (i bez value="0" który jest placeholderem)
        items = page.locator(
            ".filter-block__item.wrap-id_interest_table_bonds "
            ".choices__list--dropdown "
            ".choices__item--choice:not(.choices__item--hidden)"
        )
        count = await items.count()
        result = []
        for i in range(count):
            item = items.nth(i)
            text = await item.text_content()
            text = text.strip()
            value = await item.get_attribute("data-value")
            if value and value != "0" and text and text != "Okres odsetkowy":
                result.append({"text": text, "value": value})
        return result
    except Exception as e:
        logger.error(f"_get_visible_periods błąd: {e}")
        return []


async def _select_period(page: Page, period: dict, timeout: int) -> bool:
    """
    Otwiera dropdown okresu i klika wybrany okres po data-value.
    """
    try:
        # Otwórz dropdown
        inner = page.locator(
            ".filter-block__item.wrap-id_interest_table_bonds > .choices > .choices__inner"
        )
        await inner.click(timeout=timeout)
        await page.wait_for_timeout(300)

        # Kliknij opcję po data-value
        option = page.locator(
            f".filter-block__item.wrap-id_interest_table_bonds "
            f".choices__list--dropdown "
            f"[data-value='{period['value']}']"
        )
        if await option.count() == 0:
            # Fallback: po tekście
            option = page.get_by_role("option", name=re.compile(
                re.escape(period["text"][:20]), re.IGNORECASE
            ))
        if await option.count() == 0:
            logger.error(f"Nie znaleziono opcji okresu '{period['text']}'")
            return False

        await option.first.click(timeout=timeout)
        await page.wait_for_timeout(300)
        return True
    except Exception as e:
        logger.error(f"_select_period błąd: {e}")
        return False


async def _download_period_pdf(
    page: Page,
    period: dict,
    period_idx: int,
    ticker: str,
    data_dir: Path,
    timeout: int,
) -> Optional[Path]:
    pdf_dir = data_dir / "pdfs" / ticker
    pdf_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = pdf_dir / f"period_{period_idx:02d}_{_safe_name(period['text'])}.pdf"

    if pdf_path.exists():
        logger.debug(f"PDF już istnieje: {pdf_path}")
        return pdf_path

    # Wybierz okres
    ok = await _select_period(page, period, timeout)
    if not ok:
        return None

    # Kliknij "Pokaż wyniki"
    try:
        btn = page.locator("input[type='submit'].btn-submit")
        await btn.click(timeout=timeout)
        #await page.wait_for_load_state("networkidle", timeout=timeout)
    except Exception as e:
        logger.error(f"Nie udało się kliknąć 'Pokaż wyniki': {e}")
        return None

    # Pobierz PDF
    try:
        async with page.expect_download(timeout=timeout) as dl_info:
            link = page.get_by_role("link", name=re.compile(r"Pobierz tabelę", re.IGNORECASE))
            if await link.count() == 0:
                link = page.locator("a[href*='.pdf']")
            await link.first.click(timeout=timeout)

        download = await dl_info.value
        await download.save_as(str(pdf_path))
        logger.info(f"  Pobrano: {pdf_path.name}")
        return pdf_path

    except Exception as e:
        logger.error(f"Błąd pobierania PDF dla okresu '{period['text']}': {e}")
        return None


def _safe_name(text: str) -> str:
    return re.sub(r'[^\w\-]', '_', text)[:50]
