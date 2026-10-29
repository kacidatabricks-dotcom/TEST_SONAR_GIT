from src.mapping import AB_CLIENT_MAPPING, AP_CLIENT_MAPPING

def test_ab_mapping():
    assert AB_CLIENT_MAPPING["AB_CLIENT_ID"] == "source_client_id"
    assert AB_CLIENT_MAPPING["AB_EMAIL"] == "email"

def test_ap_mapping():
    assert AP_CLIENT_MAPPING["AP_USER_ID"] == "source_client_id"
    assert AP_CLIENT_MAPPING["AP_MAIL_ADDRESS"] == "email"
