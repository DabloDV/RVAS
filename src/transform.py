import pandas as pd
from typing import Tuple
from pathlib import Path
from datetime import datetime, timedelta, date
import math

from .config import SETTINGS
from .logger import get_logger

logger = get_logger(__name__)


def _normalize_status(s: pd.Series) -> pd.Series: #normalize status values using the domain map in SETTINGS (config.py)
#if unknown = NA
    s_str = s.astype("string").str.strip().str.lower()
    return s_str.map(SETTINGS.status_map)

def _parse_mixed_dates(s: pd.Series) -> pd.Series: #parser for booking_date

    def _parse_one_date(val):
        if pd.isna(val):
            return pd.NA

        #if date or datetime already
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, date):
            return val

        #Excel serial
        try:
            num = float(val)
            if not math.isnan(num):
                base = date(1899, 12, 30)
                return base + timedelta(days=int(num))
        except Exception:
            pass

        #Strings in known formats
        s = str(val).strip()
        if not s:
            return pd.NA
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue

        return pd.NA

    parsed = s.apply(_parse_one_date)
    remaining = int(parsed.isna().sum())
    logger.info(f"booking_date parsed (python) | remaining invalid: {remaining}")
    return parsed

def transform_doctors(df: pd.DataFrame) -> pd.DataFrame:

    expected = {"doctor_id", "name", "specialty"}
    missing = expected - set(df.columns) #every expected column not in dataframe columns means missing
    if missing:
        raise ValueError(f"Doctors missing columns: {missing}")

    out = df.copy()

    for col in ("name", "specialty"):
        out[col] = out[col].astype("string").str.strip() #every value in name or specialty must be a string. Hygiene.

    #Types
    out["doctor_id"] = pd.to_numeric(out["doctor_id"], errors="coerce").astype("Int64") #and every value in id must be an int.

    return out


def transform_appointments(df: pd.DataFrame) -> pd.DataFrame:

    expected = {"booking_id", "patient_id", "doctor_id", "booking_date", "status"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Appointments missing columns: {missing}")

    out = df.copy()

    #Remove non-digits, to Int64 and drop nulls
    raw_booking = out["booking_id"].astype("string").str.strip()
    non_digit_mask = ~raw_booking.str.fullmatch(r"\d+")
    cleaned_booking = raw_booking.str.replace(r"\D+", "", regex=True)
    changed = int(non_digit_mask.sum())
    out["booking_id"] = pd.to_numeric(cleaned_booking, errors="coerce").astype("Int64")
    null_after_clean = int(out["booking_id"].isna().sum())
    if changed:
        logger.warning(f"booking_id cleaned: {changed} rows")
    if null_after_clean:
        logger.error(f"booking_id still NULL after cleaning (rows will be dropped): {null_after_clean}")
        out = out[out["booking_id"].notna()].copy()

    #patient_id: numeric. Log and drop nulls
    out["patient_id"] = pd.to_numeric(out["patient_id"], errors="coerce").astype("Int64")
    null_patient = int(out["patient_id"].isna().sum())
    if null_patient:
        logger.warning(f"patient_id NULL (rows will be dropped): {null_patient}")
        out = out[out["patient_id"].notna()].copy()

    #doctor_id: to numeric; fractional -> invalid (Na) ---
    doc_num = pd.to_numeric(out["doctor_id"], errors="coerce")
    frac_mask = doc_num.notna() & ((doc_num % 1) != 0)
    frac_count = int(frac_mask.sum())
    if frac_count:
        logger.error(f"doctor_id has non-integer numeric values set to NULL: {frac_count}")
        doc_num.loc[frac_mask] = pd.NA
    out["doctor_id"] = doc_num.astype("Int64")

    #date parsed
    out["booking_date"] = _parse_mixed_dates(out["booking_date"])

    #drop unrealistic future dates (>= 2070-01-01)
    def _is_unrealistic(d):
        try:
            return d is not pd.NA and d.year >= 2070
        except Exception:
            return False

    future_mask = out["booking_date"].apply(_is_unrealistic)
    future_count = int(future_mask.sum())
    if future_count:
        logger.warning(f"Unrealistic booking_date dropped: {future_count}")
        out = out[~future_mask].copy()

    #dump still-invalid booking_date rows
    invalid_mask = out["booking_date"].isna()
    _dump_invalid_booking_rows(out[invalid_mask])

    #status
    before = (
        out["status"].astype("string").str.strip().str.lower()
        .value_counts(dropna=False).sort_index().to_dict()
    )
    out["status"] = _normalize_status(out["status"])
    after = out["status"].value_counts(dropna=False).sort_index().to_dict()
    logger.info(f"Status before / after normalization: {before} / {after}") #logged before and after

    return out


def quality_gates(df_doctors: pd.DataFrame, df_apps: pd.DataFrame) -> None:
    #Unique PKs check

    doc_null = int(df_doctors["doctor_id"].isna().sum()) #sum of every null doctors IDs
    doc_dup = int(df_doctors["doctor_id"].duplicated().sum()) #sum of duplicated docs ids
    app_null = int(df_apps["booking_id"].isna().sum()) #null appointments
    app_dup = int(df_apps["booking_id"].duplicated().sum()) #dup appointments

    errs = []
    if doc_null or doc_dup:
        errs.append(f"Doctors PK violations: null={doc_null}, dup={doc_dup}") #errors if
    if app_null or app_dup:
        errs.append(f"Appointments PK violations: null={app_null}, dup={app_dup}")

    #NOT NULL on appointments.doctor_id
    app_doctor_null = int(df_apps["doctor_id"].isna().sum())
    if app_doctor_null:
        errs.append(f"Appointments doctor_id NULL count: {app_doctor_null}")

    #Domain check on status after normalization
    invalid_status = int((~df_apps["status"].isin(SETTINGS.valid_status)).sum())
    if invalid_status:
        errs.append(f"Appointments invalid status values after normalization: {invalid_status}")

    #booking_date validity
    invalid_dates = int(pd.isna(df_apps["booking_date"]).sum())
    if invalid_dates:
        errs.append(f"Appointments invalid booking_date values: {invalid_dates}")

    if errs:
        #All errors shown
        message = " | ".join(errs)
        logger.error(f"Quality gates failed: {message}")
        raise ValueError(message)

    logger.info("Quality gates passed: PKs, NOT NULLs, status domain, and date validity OK.")


def enforce_fk(df_doctors: pd.DataFrame, df_apps: pd.DataFrame) -> pd.DataFrame:

    valid_doctors = set(df_doctors["doctor_id"].dropna().astype(int).tolist()) #list of doctor IDs not worth null or zero
    missing_fk_mask = ~df_apps["doctor_id"].isin(valid_doctors) #all doctor IDs as FK (appointments) not in valid doctors
    missing_fk = int(missing_fk_mask.sum()) #sum of the previous list

    if missing_fk > 0:
        rejects = df_apps[missing_fk_mask].copy()
        SETTINGS.log_dir.mkdir(parents=True, exist_ok=True)
        out_csv = Path(SETTINGS.log_dir) / "rejected_appointments_fk_doctor_missing.csv"
        rejects.to_csv(out_csv, index=False)
        logger.error(f"FK violations (appointments with unknown doctor_id): {missing_fk}")
        logger.error(f"FK violations dumped to: {out_csv} (count={len(rejects)})")

        df_apps = df_apps[~missing_fk_mask].copy()

    return df_apps

def _dump_invalid_booking_rows(df_invalid: pd.DataFrame) -> None:

    n = len(df_invalid)
    if n == 0:
        return

    preview = df_invalid.head(10).to_string(index=False)
    logger.warning(f"Invalid booking_date rows (showing up to 10 of {n}):\n{preview}")


    logs_dir = SETTINGS.log_dir
    logs_dir.mkdir(parents=True, exist_ok=True)
    out_csv = Path(logs_dir) / "invalid_booking_date_rows.csv"
    df_invalid.to_csv(out_csv, index=False)
    logger.warning(f"Full dump of invalid booking_date rows written to: {out_csv}")