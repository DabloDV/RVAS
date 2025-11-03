from pathlib import Path
from typing import Optional

import pandas as pd

from .config import SETTINGS
from .logger import get_logger

logger = get_logger(__name__)


def _ensure_exists(p: Path, env_hint: str) -> None: #error if the input file doesn't exist
    if not p.exists():
        raise FileNotFoundError(
            f"Input file not found: {p}\n"
            f"Check the .env variable {env_hint}."
        )


def extract_appointments(path: Optional[Path] = None) -> pd.DataFrame: #Reads (only) appointments' Excel as a raw DF
    src = path or SETTINGS.appointments_xlsx
    _ensure_exists(src, "APPOINTMENTS_XLSX")
    logger.info(f"Reading appointments from: {src}")

    df = pd.read_excel(src, engine="openpyxl")
    logger.info(f"Appointments read: {len(df):,} rows | columns: {list(df.columns)}")
    return df


def extract_doctors(path: Optional[Path] = None) -> pd.DataFrame: #Reads (only) doctors' Excel as a raw DF
    src = path or SETTINGS.doctors_xlsx
    _ensure_exists(src, "DOCTORS_XLSX")
    logger.info(f"Reading doctors from: {src}")

    df = pd.read_excel(src, engine="openpyxl")
    logger.info(f"Doctors read: {len(df):,} rows | columns: {list(df.columns)}")
    return df
