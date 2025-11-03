import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Set

from dotenv import load_dotenv

#Load .env
load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1] #Defines the path as absolute

def resolve_path(p: str) -> Path:   #resolves the whole path as absolute
    return (PROJECT_ROOT / p).resolve() if not os.path.isabs(p) else Path(p) #If the route was an absolute prior to this, return it so. If not, join PROJECT_ROOT with p, resolve and return

@dataclass(frozen=True)
class Settings:
    #Connection
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/vip_medical_group"
    )
    appointments_xlsx: Path = resolve_path(
        os.getenv("APPOINTMENTS_XLSX", "./data/raw/Data Engineer's Appointments Excel - VIP Medical Group.xlsx")
    )
    doctors_xlsx: Path = resolve_path(
        os.getenv("DOCTORS_XLSX", "./data/raw/Data Enginner's Doctors Excel - VIP Medical Group.xlsx")
    )
    processed_dir: Path = resolve_path(os.getenv("PROCESSED_DIR", "./data/processed"))
    log_dir: Path = resolve_path(os.getenv("LOG_DIR", "./logs"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    #Normalizing values
    status_map: Dict[str, str] = field(default_factory=lambda: { #default to avoid conflicts if SETTINGS is created again in another env
        "confirmed": "confirmed",
        "confirmed.": "confirmed",
        "cancelled": "cancelled",
        "canceled": "cancelled",
    })
    valid_status: Set[str] = field(default_factory=lambda: {"confirmed", "cancelled"})

SETTINGS = Settings()