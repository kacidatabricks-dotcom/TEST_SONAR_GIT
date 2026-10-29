def quality_rate(total, invalid):
    if total == 0:
        return 100.0
    return round((total - invalid) / total * 100, 3)

def quality_status(rate, threshold=99.0):
    return "PASS" if rate >= threshold else "FAIL"
