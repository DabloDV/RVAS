import sys
from pathlib import Path
import pandas as pd

from .config import SETTINGS
from .logger import get_logger
from .extract import extract_appointments, extract_doctors
from .transform import (
    transform_doctors,
    transform_appointments,
    quality_gates,
    enforce_fk,
)
from .load import get_conn, truncate_tables, insert_doctors, insert_appointments

logger = get_logger("etl")


def persist_final_dataset(df_doctors: pd.DataFrame, df_apps: pd.DataFrame) -> None: #persist (save) the cleaned dataset under PROCESSED_DIR
    SETTINGS.processed_dir.mkdir(parents=True, exist_ok=True)
    doctors_csv = SETTINGS.processed_dir / "final_doctors.csv"
    apps_csv = SETTINGS.processed_dir / "final_appointments.csv"

    #Writes CSVs, keeps default encoding, makes sure index isn't saved
    df_doctors.to_csv(doctors_csv, index=False)
    df_apps.to_csv(apps_csv, index=False)
    logger.info(f"Dataset written to: {SETTINGS.processed_dir}")


def main() -> int: #orchestrator
    logger.info("ETL started")

    #Extract
    df_doctors_raw = extract_doctors()
    df_apps_raw = extract_appointments()

    #Transform
    df_doctors = transform_doctors(df_doctors_raw)
    df_apps = transform_appointments(df_apps_raw)

    #Quality gates
    quality_gates(df_doctors, df_apps)

    #Enforce FK
    df_apps = enforce_fk(df_doctors, df_apps)

    #Persist final dataset
    persist_final_dataset(df_doctors, df_apps)

    #Load
    with get_conn() as conn:
        try:
            conn.autocommit = False

            #Idempotency
            truncate_tables(conn)

            #Prepares rows for bulk insert
            doc_rows = list(
                df_doctors[["doctor_id", "name", "specialty"]] #respects order in load
                .astype(object)
                .itertuples(index=False, name=None)
            )
            app_rows = list(
                df_apps[["booking_id", "patient_id", "doctor_id", "booking_date", "status"]]
                .astype(object)
                .itertuples(index=False, name=None)
            )

            n_docs = insert_doctors(conn, doc_rows)
            n_apps = insert_appointments(conn, app_rows)

            conn.commit()
            logger.info(f"Load committed. Doctors: {n_docs:,} | Appointments: {n_apps:,}")
        except Exception:
            conn.rollback()
            logger.exception("Load failed. Transaction rolled back.")
            return 2

    logger.info("ETL finished successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())