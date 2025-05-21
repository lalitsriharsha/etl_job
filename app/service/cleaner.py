import re
from dateutil import parser

def validate_email(email):
    return email if re.match(r"[^@]+@[^@]+\.[^@]+", str(email)) else None

def validate_phone(phone):
    return phone if re.match(r"^\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$", str(phone)) else None

def parse_date(date_str):
    try:
        return str(parser.parse(str(date_str)).date())
    except Exception:
        return None


def clean_data(row):
    # Normalize keys
    row = {k.lower(): v for k, v in row.items()}

    # Validate fields
    row["email"] = validate_email(row.get("email"))
    row["cell"] = validate_phone(row.get("cell"))
    row["dob"] = parse_date(row.get("dob"))

    # Cast 'pid' and 'pcode'
    try:
        row["pid"] = int(float(row.get("pid", 0)))
    except Exception:
        row["pid"] = 0

    try:
        row["pcode"] = int(float(row.get("pcode", 0)))
    except Exception:
        row["pcode"] = 0

    return row
