from src.matching import calculate_match_score, classify_match

def test_full_match():
    assert calculate_match_score(True, True, True, True) == 110

def test_duplicate_classification():
    assert classify_match(110) == "DOUBLON"

def test_review_classification():
    assert classify_match(60) == "A_VERIFIER"

def test_different_classification():
    assert classify_match(10) == "DIFFERENT"
