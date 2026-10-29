from src.quality import quality_rate, quality_status

def test_quality_rate():
    assert quality_rate(1000, 10) == 99.0

def test_quality_pass():
    assert quality_status(99.5) == "PASS"

def test_quality_fail():
    assert quality_status(98.0) == "FAIL"
