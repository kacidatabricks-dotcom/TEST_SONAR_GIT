def calculate_match_score(
    same_email=False,
    same_phone=False,
    same_name=False,
    same_birth_date=False,
):
    return (
        (50 if same_email else 0)
        + (30 if same_phone else 0)
        + (10 if same_name else 0)
        + (20 if same_birth_date else 0)
    )

def classify_match(score):
    if score >= 80:
        return "DOUBLON"
    if score >= 50:
        return "A_VERIFIER"
    return "DIFFERENT"
