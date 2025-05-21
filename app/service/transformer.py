import re
from datetime import datetime
import math

INT_COLUMNS = {"pid", "pcode"}
FLOAT_COLUMNS = {"gestage"}
EMAIL_COLUMNS = {"email"}
PHONE_COLUMNS = {"cell"}
DATE_COLUMNS = {"dob"}

def is_missing(val):
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    if isinstance(val, str) and val.strip().lower() in {"", "nan"}:
        return True
    return False

def mask_email(email):
    if not email:
        return None
    parts = email.split("@")
    return f"{parts[0][0]}***@{parts[1]}" if len(parts) > 1 else None

def mask_phone(phone):
    return re.sub(r"\d(?=\d{2})", "*", phone) if phone else None

def transform_data(row):
    for col in INT_COLUMNS:
        val = row.get(col)
        row[col] = int(float(val)) if not is_missing(val) else 0

    for col in FLOAT_COLUMNS:
        val = row.get(col)
        row[col] = round(float(val), 3) if not is_missing(val) else 0.0

    for col in EMAIL_COLUMNS:
        row[col] = mask_email(row.get(col))

    for col in PHONE_COLUMNS:
        row[col] = mask_phone(row.get(col))

    for col in DATE_COLUMNS:
        val = row.get(col)
        try:
            row[col] = str(datetime.strptime(str(val), "%Y-%m-%d").date()) if not is_missing(val) else None
        except Exception:
            row[col] = None

    return row
