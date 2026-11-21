
import importlib
load_ap_data = importlib.import_module("sonar-fonc-ingestion-des-donnees-assureplus").load_ap_data

def test_load_ap_data(spark):
    # Sans data_dir, la fonction retombe sur DATA/SAMPLE/APASSURANCE du dépôt.
    # L'ingestion lit les CSV réels (pas les 10 lignes d'exemple historiques) :
    # on vérifie qu'ils sont non vides et que les colonnes clés sont présentes.
    df_clients, df_contrats, df_sinistres, df_payments = load_ap_data(spark)

    for i, df in enumerate([df_clients, df_contrats, df_sinistres, df_payments]):
        assert df.count() > 0, f"Le DataFrame #{i} ne doit pas être vide"

    assert "AP_USER_ID" in df_clients.columns
    assert "AP_CONTRACT_REF" in df_contrats.columns
    assert "AP_SINISTRE_NUM" in df_sinistres.columns
    assert "AP_PAYMENT_REF" in df_payments.columns