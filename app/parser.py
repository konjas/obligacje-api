"""
Parser tabel odsetkowych w formacie PDF ze strony obligacjeskarbowe.pl.

Tabela w PDF ma strukturę:
  - Nagłówek z miesiącami: 2026-03, 2026-04, ..., 2027-03
  - Kolumna DZIEŃ M-CA (dzień miesiąca 01-31)
  - Wartości odsetek w PLN dla 1 sztuki obligacji (z przecinkiem jako separatorem dziesiętnym)

Wynikowy format: lista (day_offset, interest) gdzie day_offset=0 oznacza dzień zakupu.
"""
import calendar
import logging
import re
from datetime import date
from pathlib import Path
from typing import List, Optional, Tuple

import pdfplumber

logger = logging.getLogger(__name__)


def parse_interest_pdf(pdf_path: str) -> Optional[dict]:
    """
    Parsuje PDF z tabelą odsetkową.

    Zwraca:
    {
        "ticker":        "EDO0336",
        "rate_pct":      5.60,
        "period_start":  "2026-03-01",   # pierwszy dzień sprzedaży
        "entries":       [(0, 0.00), (1, 0.02), ...],  # (day_offset, odsetki PLN)
    }
    lub None w przypadku błędu.
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[0]
            text = page.extract_text() or ""

            ticker = _extract_ticker(text)
            rate_pct = _extract_rate(text)
            period_start = _extract_period_start(text)

            # Próba parsowania strukturalnego, fallback na tekst
            entries = _parse_entries(page, text)

            if not entries:
                logger.warning(f"Brak danych w {pdf_path}")
                return None

            return {
                "ticker": ticker,
                "rate_pct": rate_pct,
                "period_start": period_start,
                "entries": entries,
            }
    except Exception as e:
        logger.error(f"Błąd parsowania {pdf_path}: {e}", exc_info=True)
        return None


# ── Ekstrakcja metadanych z nagłówka ────────────────────────────────────────

def _extract_ticker(text: str) -> Optional[str]:
    m = re.search(r'\b([A-Z]{2,3}\d{4})\b', text)
    return m.group(1) if m else None


def _extract_rate(text: str) -> Optional[float]:
    m = re.search(r'Oprocentowanie[^:]*:\s*([\d,]+)\s*%', text)
    if m:
        return float(m.group(1).replace(',', '.'))
    # fallback: szukaj "X,XX%" gdziekolwiek
    m = re.search(r'(\d{1,2},\d{2})\s*%', text)
    return float(m.group(1).replace(',', '.')) if m else None


def _extract_period_start(text: str) -> Optional[str]:
    # "NABYTYCH W DNIACH OD 2026-03-01 DO ..."
    m = re.search(r'OD\s+(\d{4}-\d{2}-\d{2})', text)
    return m.group(1) if m else None


# ── Parsowanie tabeli ────────────────────────────────────────────────────────

def _parse_entries(page, text: str) -> List[Tuple[int, float]]:
    """Próbuje parsować tabelę strukturalnie, fallback na raw text."""
    # Próba 1: pdfplumber extract_tables
    try:
        tables = page.extract_tables()
        if tables:
            result = _parse_structured_table(tables[0])
            if result:
                logger.debug(f"Sparsowano tabelę strukturalnie ({len(result)} wpisów)")
                return result
    except Exception as e:
        logger.debug(f"Parsowanie strukturalne nie powiodło się: {e}")

    # Próba 2: raw text
    result = _parse_text(text)
    if result:
        logger.debug(f"Sparsowano tekst ({len(result)} wpisów)")
    return result


def _decode_cell(cell: str) -> str:
    """
    Dekoduje tekst z komórki – obsługuje obrócone nagłówki starego formatu.
    Stary format: miesiąc zapisany pionowo, każdy znak w osobnej linii,
    np. '9\n0\n2-\n2\n0\n2' → odwrócone → '2022-09'.
    """
    if not cell:
        return ""
    tokens = cell.split('\n')
    # Heurystyka: jeśli każdy token ma <= 2 znaki i jest ich >= 4,
    # to prawdopodobnie obrócony tekst
    if len(tokens) >= 4 and all(len(t) <= 2 for t in tokens):
        return ''.join(reversed(tokens))
    return cell.strip()


def _parse_structured_table(table: list) -> Optional[List[Tuple[int, float]]]:
    """Parsuje tabelę zwróconą przez pdfplumber – obsługuje nowy i stary format."""
    if not table or len(table) < 2:
        return None

    MONTH_RE = re.compile(r'^\d{4}-\d{2}$')

    # Znajdź wiersz nagłówkowy z miesiącami (po odkodowaniu)
    header_row_idx = None
    month_col_map: dict[int, date] = {}

    for row_idx, row in enumerate(table):
        if not row:
            continue
        found = {}
        for col_idx, cell in enumerate(row):
            if not cell:
                continue
            decoded = _decode_cell(str(cell))
            if MONTH_RE.match(decoded):
                yr, mo = int(decoded[:4]), int(decoded[5:7])
                found[col_idx] = date(yr, mo, 1)
        if len(found) >= 3:
            header_row_idx = row_idx
            month_col_map = found
            break

    if header_row_idx is None:
        return None

    # Ustal kolumnę z dniem miesiąca
    all_cols = set(range(len(table[header_row_idx])))
    day_col = _find_day_column(table, header_row_idx, all_cols - set(month_col_map.keys()))

    # Zbierz wartości
    cal_data: dict[date, float] = {}

    for row in table[header_row_idx + 1:]:
        if not row:
            continue
        raw_day = str(row[day_col]).strip() if day_col is not None and day_col < len(row) else ""
        if not re.match(r'^\d{1,2}$', raw_day):
            continue
        day = int(raw_day)

        for col_idx, month_start in month_col_map.items():
            if col_idx >= len(row):
                continue
            cell = row[col_idx]
            if cell is None or str(cell).strip() == '':
                continue
            try:
                val = float(str(cell).strip().replace(',', '.').replace('\xa0', ''))
            except ValueError:
                continue
            max_day = calendar.monthrange(month_start.year, month_start.month)[1]
            if 1 <= day <= max_day:
                cal_data[date(month_start.year, month_start.month, day)] = val

    return _to_offset_list(cal_data)


def _find_day_column(table: list, header_row_idx: int, candidates: set) -> Optional[int]:
    """Spośród kandydatów wybiera kolumnę z dniami miesiąca."""
    for ci in sorted(candidates):
        hits = 0
        for row in table[header_row_idx + 1: header_row_idx + 6]:
            if not row or ci >= len(row):
                continue
            cell = str(row[ci] or '').strip()
            if re.match(r'^\d{1,2}$', cell) and 1 <= int(cell) <= 31:
                hits += 1
        if hits >= 2:
            return ci
    return min(candidates) if candidates else None


def _parse_text(text: str) -> List[Tuple[int, float]]:
    """Fallback: parsuje tekst wyciągnięty z PDF."""
    lines = text.split('\n')

    MONTH_RE = re.compile(r'(\d{4}-\d{2})')

    # Znajdź nagłówek z miesiącami
    months: List[date] = []
    header_line_idx = -1
    for i, line in enumerate(lines):
        found = MONTH_RE.findall(line)
        if len(found) >= 3:
            header_line_idx = i
            months = [date(int(m[:4]), int(m[5:7]), 1) for m in found]
            break

    if not months:
        return []

    cal_data: dict[date, float] = {}
    NUM_RE = re.compile(r'[\d,]+')

    for line in lines[header_line_idx + 1:]:
        stripped = line.strip()
        if not stripped:
            continue
        parts = stripped.split()
        if not parts:
            continue

        # Pierwsza kolumna to dzień miesiąca
        if not re.match(r'^\d{1,2}$', parts[0]):
            continue
        day = int(parts[0])
        if not (1 <= day <= 31):
            continue

        # Pozostałe to wartości odsetek (może być mniej niż miesięcy – puste komórki)
        values = []
        for p in parts[1:]:
            p = p.replace(',', '.')
            try:
                values.append(float(p))
            except ValueError:
                break  # koniec danych numerycznych

        for i, val in enumerate(values):
            if i >= len(months):
                break
            m = months[i]
            max_day = calendar.monthrange(m.year, m.month)[1]
            if 1 <= day <= max_day:
                cal_data[date(m.year, m.month, day)] = val

    return _to_offset_list(cal_data)


def _to_offset_list(cal_data: dict) -> List[Tuple[int, float]]:
    """Konwertuje słownik {data: odsetki} na listę (day_offset, odsetki) posortowaną rosnąco."""
    if not cal_data:
        return []
    sorted_dates = sorted(cal_data.keys())
    return [(i, cal_data[d]) for i, d in enumerate(sorted_dates)]
