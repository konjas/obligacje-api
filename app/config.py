from dataclasses import dataclass, field
from pathlib import Path
from typing import List
import yaml


@dataclass
class AppConfig:
    tickers: List[str] = field(default_factory=list)
    data_dir: str = "/data"
    check_day: int = 1
    check_hour: int = 6
    scraper_timeout: int = 45000
    headless: bool = True


def load_config(path: str = "/config/config.yaml") -> AppConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    # Walidacja tickerów
    tickers = raw.get("tickers", [])
    if not isinstance(tickers, list):
        tickers = [tickers]
    raw["tickers"] = [str(t).strip().upper() for t in tickers if t]
    return AppConfig(**{k: v for k, v in raw.items() if k in AppConfig.__dataclass_fields__})
