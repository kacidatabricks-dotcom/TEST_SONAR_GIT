# ============================================================================
# Tests du module SONAR_FONC_PROTECTION_CONTRATS_SINISTRES_PAYMENTS (protection_bronze)
# ============================================================================

#=======
# NB : ce module n'importe pas F/dbutils dans son propre fichier ; il s'appuie sur
# le namespace du notebook. On injecte donc F et dbutils dans son namespace avant
# de tester, puis on appelle la fonction via la référence du module.

import hashlib

import importlib
_protection_module = importlib.import_module("sonar-fonc-protection-contrats-sinistres-payments")
from pyspark.sql import functions as F

# Sur un runner GitHub Actions, dbutils n'existe pas (outil Databricks uniquement).
# On fournit alors un stub dont le secret est une constante : le hachage reste
# vérifiable hors Databricks, et le vrai dbutils est utilisé s'il est présent.
try:
    dbutils  # présent uniquement dans un notebook Databricks
except NameError:
    class _SecretsStub:
        @staticmethod
        def get(scope, key):
            return "SEL_TEST_LOCAL_CI"

    class _DBUtilsStub:
        secrets = _SecretsStub()

    dbutils = _DBUtilsStub()


def _activer_module_protection():
    """Injecte F et dbutils dans le namespace du module pour le rendre appelable."""
    _protection_module.F = F
    _protection_module.dbutils = dbutils
    return _protection_module.protection_bronze


def _get_salt():
    return dbutils.secrets.get(scope="PROJET_BIG_DATA_STUDI", key="hash_salt")


def test_protection_bronze_oracle():
    """Branche Oracle : hachage des colonnes sensibles, suppression des originales,
    conservation de l'identifiant, hash = sha2(concat(valeur, salt))."""
    protection_bronze = _activer_module_protection()
    salt = _get_salt()

    df = spark.createDataFrame([
        (1, "MARTIN", "Jean", "jean.martin@example.fr", "0612345678",
         "15 rue de Paris", "75001", "1234567890123", "1985-03-15", "ACTIF")
    ], ["AB_CLIENT_ID", "AB_NOM", "AB_PRENOM", "AB_EMAIL", "AB_TELEPHONE",
        "AB_ADRESSE", "AB_CODE_POSTAL", "AB_NUM_FISCAL", "AB_DATE_NAISSANCE", "AB_STATUT_CLIENT"])

    df_protege = protection_bronze(df, "Oracle")

    # 1. Les colonnes hachées sont présentes
    for col_h in ["Nom_Hached", "Prenom_Hached", "Email_Hached", "Telephone_Hached",
                  "Adresse_Hached", "Num_fiscal_Hached", "Date_de_naissance_Hached"]:
        assert col_h in df_protege.columns, f"Colonne hachée manquante : {col_h}"

    # 2. Les colonnes sensibles d'origine sont supprimées
    for col_s in ["AB_NOM", "AB_PRENOM", "AB_EMAIL", "AB_TELEPHONE",
                  "AB_ADRESSE", "AB_NUM_FISCAL", "AB_DATE_NAISSANCE"]:
        assert col_s not in df_protege.columns, f"Colonne sensible non supprimée : {col_s}"

    # 3. L'identifiant (non sensible) est conservé
    assert "AB_CLIENT_ID" in df_protege.columns, "L'identifiant client doit être conservé"
    assert df_protege.count() == 1

    # 4. Le hash est déterministe et utilise bien le sel
    row = df_protege.collect()[0]
    attendu = hashlib.sha256(("MARTIN" + salt).encode()).hexdigest()
    assert row.Nom_Hached == attendu, "Le hash doit être sha2(concat(valeur, salt))"
    sans_sel = hashlib.sha256("MARTIN".encode()).hexdigest()
    assert row.Nom_Hached != sans_sel, "Le sel doit être appliqué (hash != sha2 sans sel)"


def test_protection_bronze_sqlserver():
    """Branche SQLServer (toute source != Oracle) : mêmes garanties sur les colonnes AP_*."""
    protection_bronze = _activer_module_protection()
    salt = _get_salt()

    df = spark.createDataFrame([
        (200001, "MARTIN", "Jean", "Jean Martin", "jean.martin@example.fr",
         "0612345678", "15 rue de Paris", "1985-03-15", "ACTIF")
    ], ["AP_USER_ID", "AP_NOM", "AP_PRENOM", "AP_FULL_NAME", "AP_MAIL_ADDRESS",
        "AP_PHONE_NUMBER", "AP_STREET_ADDRESS", "AP_BIRTH_DATE", "AP_CUSTOMER_STATUS"])

    df_protege = protection_bronze(df, "SQLServer")

    for col_h in ["Nom_Hached", "Prenom_Hached", "Email_Hached", "Telephone_Hached",
                  "Street_Hached", "FULL_NAME_Hached", "Date_de_naissance_Hached"]:
        assert col_h in df_protege.columns, f"Colonne hachée manquante : {col_h}"

    for col_s in ["AP_NOM", "AP_PRENOM", "AP_FULL_NAME", "AP_MAIL_ADDRESS",
                  "AP_PHONE_NUMBER", "AP_STREET_ADDRESS", "AP_BIRTH_DATE"]:
        assert col_s not in df_protege.columns, f"Colonne sensible non supprimée : {col_s}"

    assert "AP_USER_ID" in df_protege.columns, "L'identifiant utilisateur doit être conservé"

    row = df_protege.collect()[0]
    attendu = hashlib.sha256(("15 rue de Paris" + salt).encode()).hexdigest()
    assert row.Street_Hached == attendu, "Le hash de l'adresse doit être sha2(concat(valeur, salt))"
