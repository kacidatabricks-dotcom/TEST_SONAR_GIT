from SONAR_FONC_UNIFICATION_ET_GOLD_CONTRAT import unify_and_map_contrats

def test_unify_and_map_contrats():
    # Création des DataFrames de test
    df_ab_contrats = spark.createDataFrame([
        ("P123", "CL001", "AUTO", "ACTIVE", 1000)
    ], ["AB_POLICY_NUMBER", "AB_CLIENT_ID", "AB_TYPE_ASSURANCE", "AB_STATUT_CONTRAT", "AB_PRIME_ANNUELLE"])

    df_ap_contrats = spark.createDataFrame([
        ("C456", "CL002", "HOME", "INACTIVE", 500)
    ], ["AP_CONTRACT_REF", "AP_USER_ID", "AP_PRODUCT_CODE", "AP_CONTRACT_STATE", "AP_MONTHLY_PREMIUM"])

    dim_clients = spark.createDataFrame([
        ("CL001", "ORACLE", "M001"),
        ("CL002", "SQL", "M002")
    ], ["ORIGINAL_ID", "SOURCE_SYSTEM", "MASTER_CLIENT_ID"])

    result = unify_and_map_contrats(df_ab_contrats, df_ap_contrats, dim_clients)
    
    assert result is not None, "Le DataFrame de résultat ne doit pas être None"
    assert result.count() == 2, "Le DataFrame doit contenir deux contrats"
    rows = {row.CONTRAT_ID: row for row in result.collect()}
    assert "P123" in rows and "C456" in rows, "Les identifiants de contrats doivent être présents"
    # Le mapping vers le master client est correct pour chaque source
    assert rows["P123"].MASTER_CLIENT_ID == "M001"
    assert rows["C456"].MASTER_CLIENT_ID == "M002"
    # La colonne TYPE_CONTRAT est alimentée depuis chaque source
    assert rows["P123"].TYPE_CONTRAT == "AUTO"
    assert rows["C456"].TYPE_CONTRAT == "HOME"
    
#tahc test client absent


def test_unify_and_map_contrats_client_absent():
    """Un contrat dont le client n'est pas dans dim_clients est écarté
    (jointure interne contrats <-> dim_clients)."""
    df_ab_contrats = spark.createDataFrame([
        ("P999", "CL999", "AUTO", "ACTIVE", 1000)
    ], ["AB_POLICY_NUMBER", "AB_CLIENT_ID", "AB_TYPE_ASSURANCE", "AB_STATUT_CONTRAT", "AB_PRIME_ANNUELLE"])

    df_ap_contrats = spark.createDataFrame(
        [], "AP_CONTRACT_REF STRING, AP_USER_ID STRING, AP_PRODUCT_CODE STRING, AP_CONTRACT_STATE STRING, AP_MONTHLY_PREMIUM DOUBLE"
    )

    dim_clients = spark.createDataFrame([
        ("CL001", "ORACLE", "M001")
    ], ["ORIGINAL_ID", "SOURCE_SYSTEM", "MASTER_CLIENT_ID"])

    result = unify_and_map_contrats(df_ab_contrats, df_ap_contrats, dim_clients)
    assert result.count() == 0, "Un contrat sans client rattaché doit être écarté"


def test_unify_and_map_contrats_erreur():
    """Une colonne obligatoire absente doit faire échouer la fonction
    (l'exception est relancée par le module, pas avalée)."""
    df_ko = spark.createDataFrame([("X", 1)], ["MAUVAISE_COLONNE", "AUTRE"])
    dim_clients = spark.createDataFrame(
        [], "ORIGINAL_ID STRING, SOURCE_SYSTEM STRING, MASTER_CLIENT_ID STRING"
    )

    erreur_levee = False
    try:
        unify_and_map_contrats(df_ko, df_ko, dim_clients)
    except Exception:
        erreur_levee = True

    assert erreur_levee, "Une colonne manquante doit provoquer une erreur relancée"