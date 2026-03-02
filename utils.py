import os
import magic
import shutil
from pathlib import Path

SANDBOX_DIR = "sandbox"
MAX_FILE_SIZE_MB = 500


def ensure_sandbox():
    os.makedirs(SANDBOX_DIR, exist_ok=True)


def validate_file_size(path):
    size_mb = Path(path).stat().st_size / (1024 * 1024)
    if size_mb > MAX_FILE_SIZE_MB:
        raise ValueError("File exceeds 500MB limit")


def detect_file_type(path):
    mime = magic.from_file(path, mime=True)

    if "csv" in mime or path.endswith(".csv"):
        return "csv"
    if "excel" in mime or path.endswith((".xls", ".xlsx")):
        return "excel"
    if "json" in mime or path.endswith(".json"):
        return "json"

    return "unknown"


def copy_to_sandbox(path):
    ensure_sandbox()
    dest = os.path.join(SANDBOX_DIR, os.path.basename(path))
    shutil.copy(path, dest)
    return dest