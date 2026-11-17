# **************************************************************************
# Tests du module SONAR_FONC_SUPPRESSION_INCOHERENCES
# (suppression_incoherences_clients  et  suppression_incoherences générique)
# ============================================================================
from SONAR_FONC_SUPPRESSION_INCOHERENCES import (
    suppression_incoherences,
    suppression_incoherences_clients,
)
from pyspark.sql import functions as F


def test_suppression_incoherences_clients():
    """Séparation valide/rejetée : dates invalides, dates dans le futur,
    création antérieure à la naissance, identifiant dupliqué."""
    df = spark.createDataFrame([
        (1, "MARTIN", "1985-03-15", "2020-01-15"),   # valide
        (2, "DUPONT", "31/02/1990", "2020-03-20"),   # date de naissance invalide
        (3, "DURAND", "2099-01-01", "2020-03-20"),   # naissance dans le futur
        (4, "BLANC",  "2000-01-01", "2050-01-01"),   # création dans le futur
        (5, "PETIT",  "2000-01-01", "1990-01-01"),   # création antérieure à la naissance
        (6, "MOREAU", "1990-01-01", "2020-01-01"),   # identifiant dupliqué (x2)
        (6, "MOREAU", "1990-01-01", "2020-02-01"),
    ], ["AB_CLIENT_ID", "AB_NOM", "AB_DATE_NAISSANCE", "AB_DATE_CREATION"])

    # NB : la fonction étend la liste reçue (mutation) -> on passe une liste neuve
    controles = [
        (F.col("AB_NOM").isNull(), "Nom manquant"),
    ]

    clean, rejected = suppression_incoherences_clients(
        df,
        controles,
        date_naissance="AB_DATE_NAISSANCE",
        date_creation="AB_DATE_CREATION",
        cle_primaire="AB_CLIENT_ID",
    )

    assert clean.count() == 1, "Un seul client valide est attendu (MARTIN)"
    assert rejected.count() == 6, "Six clients incohérents sont attendus"
    assert clean.collect()[0].AB_CLIENT_ID == 1

    # Chaque motif d'incohérence attendu apparaît bien chez les rejetés
    motifs = " | ".join(sorted({row._MOTIF_INCOHERENCE for row in rejected.collect()}))
    for attendu in [
        "Date de naissance invalide",
        "Date de naissance dans le futur",
        "Date de création dans le futur",
        "Date de création antérieure à la naissance",
        "Identifiant client dupliqué",
    ]:
        assert attendu in motifs, f"Motif attendu absent : {attendu}"

    # Les colonnes techniques ne doivent pas rester dans le DataFrame propre
    for col_tech in ["__birth_date", "__creation_date", "__duplicate_count", "_MOTIF_INCOHERENCE"]:
        assert col_tech not in clean.columns, f"Colonne technique non supprimée : {col_tech}"


def test_suppression_incoherences():
    """Fonction générique : dates invalides, clé primaire dupliquée,
    référence de contrat inexistante, colonnes techniques supprimées."""
    df = spark.createDataFrame([
        ("P0", 101, "2020-01-15"),   # valide : date ok, contrat existant, clé unique
        ("P2", 102, "13/45/2020"),   # date invalide
        ("P3", 103, "2021-05-01"),   # référence de contrat inexistante
        ("P1", 104, "2021-06-01"),   # clé primaire dupliquée (x2)
        ("P1", 105, "2021-07-01"),
    ], ["AB_POLICY_NUMBER", "AB_CLIENT_ID", "AB_DATE_DEBUT"])

    contrats_valides = spark.createDataFrame([
        ("P0",), ("P99",)
    ], ["AB_POLICY_NUMBER"])

    controles = [
        (F.col("AB_CLIENT_ID").isNull() | (F.col("AB_CLIENT_ID") <= 0), "Identifiant client invalide"),
    ]

    valides, invalides = suppression_incoherences(
        df,
        controles,
        colonnes_dates=["AB_DATE_DEBUT"],
        cle_primaire="AB_POLICY_NUMBER",
        contrats_valides=contrats_valides,
    )

    assert valides.count() == 1, "Un seul contrat valide est attendu (P0)"
    assert invalides.count() == 4, "Quatre contrats incohérents sont attendus"
    assert valides.collect()[0].AB_POLICY_NUMBER == "P0"

    motifs = " | ".join(sorted({row._MOTIF_INCOHERENCE for row in invalides.collect()}))
    for attendu in [
        "Date invalide : AB_DATE_DEBUT",
        "Référence de contrat inexistante",
        "Clé primaire dupliquée : AB_POLICY_NUMBER",
    ]:
        assert attendu in motifs, f"Motif attendu absent : {attendu}"

    # Les colonnes techniques ne doivent pas rester dans le DataFrame valide
    for col_tech in ["__date_AB_DATE_DEBUT", "__nombre_doublons", "__policy_exists", "_MOTIF_INCOHERENCE"]:
        assert col_tech not in valides.columns, f"Colonne technique non supprimée : {col_tech}"
