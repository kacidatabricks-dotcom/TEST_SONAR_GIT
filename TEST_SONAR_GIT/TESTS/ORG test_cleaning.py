from src.cleaning import (
    normalize_email,
    normalize_phone,
    normalize_name,
)

def test_normalize_email():
    assert normalize_email(" Jean.Dupont@GMAIL.COM ") == "jean.dupont@gmail.com"

def test_normalize_email_null():
    assert normalize_email(None) is None

def test_normalize_phone():
    assert normalize_phone("06 12 34 56 78") == "0612345678"

def test_normalize_name():
    assert normalize_name("  DUPONT   jean ") == "Dupont jean"
