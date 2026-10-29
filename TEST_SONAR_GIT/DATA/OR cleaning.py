import re
import unicodedata

def normalize_email(email):
    if email is None:
        return None
    value = email.strip().lower()
    return value or None

def normalize_phone(phone):
    if phone is None:
        return None
    value = re.sub(r"[^0-9+]", "", phone.strip())
    return value or None

def normalize_text(value):
    if value is None:
        return None
    value = " ".join(value.strip().split())
    return value or None

def normalize_name(value):
    value = normalize_text(value)
    return value.title() if value else None

def remove_accents(value):
    if value is None:
        return None
    return "".join(
        c for c in unicodedata.normalize("NFD", value)
        if unicodedata.category(c) != "Mn"
    )
