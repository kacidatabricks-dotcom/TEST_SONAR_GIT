# Tests du module sonar-fonc-ingestion-flux (ingestion en flux simulé).
#
# Fonctions testées : decouper_csv_en_blocs, _trouver_csv, _schema_texte,
# _appliquer_transformations (typage numérique, règles métier, hachage PII).
# Ces tests sont portables : fichiers CSV créés dans des dossiers
# temporaires, sel de hachage constant — aucune dépendance aux vraies
# données ni aux tables Unity Catalog.
# Les fonctions de streaming proprement dites (demarrer/attendre/
# reinitialiser) écrivent dans workspace.bronze.*_flux : elles sont
# validées par la cellule « Ingestion en flux simulé » du notebook.

import hashlib
import importlib
import tempfile
from pathlib import Path

from pyspark.sql import functions as F

_flux = importlib.import_module("sonar-fonc-ingestion-flux")


def _sha2(valeur, sel="sel-test-local-ci"):
    """Hash de référence calculé comme en mode batch : sha2(valeur || sel)."""
    return hashlib.sha256((valeur + sel).encode("utf-8")).hexdigest()


def _ecrire_csv_de_test(dossier, nom, entetes, lignes):
    """Crée un petit CSV de test et retourne son chemin."""
    chemin = Path(dossier) / nom
    with open(chemin, "w", encoding="utf-8", newline="") as sortie:
        sortie.write(",".join(entetes) + "\n")
        sortie.writelines(",".join(ligne) + "\n" for ligne in lignes)
    return chemin


def test_decouper_csv_en_blocs():
    """5 lignes en blocs de 2 -> 3 blocs (2+2+1), en-tête répété dans
    chaque bloc, contenu total préservé à l'identique."""
    with tempfile.TemporaryDirectory() as dossier:
        source = _ecrire_csv_de_test(
            dossier,
            "clients.csv",
            ["ID", "NOM"],
            [[str(i), f"nom{i}"] for i in range(1, 6)],
        )
        nb_lignes = _flux.decouper_csv_en_blocs(
            source, Path(dossier) / "blocs", taille_bloc=2
        )
        assert nb_lignes == 5

        blocs = sorted((Path(dossier) / "blocs").glob("bloc_*.csv"))
        assert [b.name for b in blocs] == [
            "bloc_000.csv",
            "bloc_001.csv",
            "bloc_002.csv",
        ]

        lignes_lues = []
        for bloc in blocs:
            contenu = bloc.read_text(encoding="utf-8").splitlines()
            assert contenu[0] == "ID,NOM"  # en-tête répété dans chaque bloc
            assert len(contenu) - 1 <= 2  # taille de bloc respectée
            lignes_lues.extend(contenu[1:])
        assert lignes_lues == [f"{i},nom{i}" for i in range(1, 6)]


def test_decouper_csv_purge_anciens_blocs():
    """Un nouveau découpage remplace les anciens blocs : aucune double
    ingestion possible lors d'une réexécution."""
    with tempfile.TemporaryDirectory() as dossier:
        source = _ecrire_csv_de_test(
            dossier,
            "contrats.csv",
            ["REF", "MONTANT"],
            [[f"R{i}", str(i)] for i in range(1, 5)],
        )
        dossier_blocs = Path(dossier) / "blocs"

        _flux.decouper_csv_en_blocs(source, dossier_blocs, taille_bloc=1)
        assert len(list(dossier_blocs.glob("bloc_*.csv"))) == 4

        _flux.decouper_csv_en_blocs(source, dossier_blocs, taille_bloc=4)
        blocs = list(dossier_blocs.glob("bloc_*.csv"))
        assert [b.name for b in blocs] == ["bloc_000.csv"]


def test_trouver_csv_insensible_a_la_casse():
    """Le CSV d'une entité est trouvé quelle que soit la casse du nom
    (ABASSURANCE/clients.csv vs APASSURANCE/Clients.csv)."""
    with tempfile.TemporaryDirectory() as dossier:
        _ecrire_csv_de_test(dossier, "Clients.csv", ["ID"], [["1"]])
        chemin = _flux._trouver_csv(dossier, "clients")
        assert chemin.name == "Clients.csv"

        try:
            _flux._trouver_csv(dossier, "sinistres")
        except FileNotFoundError:
            pass
        else:
            raise AssertionError(
                "FileNotFoundError attendue pour une entité absente"
            )


def test_schema_texte():
    """Le schéma de flux est construit depuis l'en-tête réel du CSV :
    toutes les colonnes en chaînes, dans l'ordre du fichier."""
    with tempfile.TemporaryDirectory() as dossier:
        source = _ecrire_csv_de_test(
            dossier,
            "clients.csv",
            ["AB_CLIENT_ID", "AB_NOM", "AB_PRIME"],
            [["1", "Martin", "10.5"]],
        )
        schema = _flux._schema_texte(source)
        assert [champ.name for champ in schema.fields] == [
            "AB_CLIENT_ID",
            "AB_NOM",
            "AB_PRIME",
        ]
        assert all(
            champ.dataType.simpleString() == "string" for champ in schema.fields
        )


def test_appliquer_transformations_clients_oracle():
    """Clients Oracle : cast de l'identifiant, hachage PII identique au
    batch (sha2 + sel), suppression des colonnes PII d'origine."""
    df = spark.createDataFrame(
        [("1", "Martin", "Jean", "j.martin@example.fr")],
        "AB_CLIENT_ID string, AB_NOM string, AB_PRENOM string, AB_EMAIL string",
    )
    resultat = _flux._appliquer_transformations(
        df, "oracle", "clients", salt="sel-test-local-ci"
    )

    # Typage numérique
    assert resultat.schema["AB_CLIENT_ID"].dataType.simpleString() == "int"
    assert resultat.collect()[0]["AB_CLIENT_ID"] == 1

    # Hachage identique au mode batch
    attendu = (
        spark.createDataFrame([("Martin",)], "N string")
        .select(F.sha2(F.concat(F.col("N"), F.lit("sel-test-local-ci")), 256))
        .collect()[0][0]
    )
    assert resultat.select("Nom_Hached").collect()[0][0] == attendu

    # Colonnes PII d'origine supprimées, colonnes hachées présentes
    assert "AB_NOM" not in resultat.columns
    assert "AB_EMAIL" not in resultat.columns
    assert "Nom_Hached" in resultat.columns
    assert "Email_Hached" in resultat.columns


def test_appliquer_transformations_clients_assureplus():
    """Clients AssurePlus : scission du nom complet, hachage PII,
    suppression des colonnes PII d'origine."""
    df = spark.createDataFrame(
        [("7", "Jean Dupont", "jean.dupont@example.fr", "1990-01-01")],
        "AP_USER_ID string, AP_FULL_NAME string, AP_MAIL_ADDRESS string, "
        "AP_BIRTH_DATE string",
    )
    resultat = _flux._appliquer_transformations(
        df, "assureplus", "clients", salt="sel-test-local-ci"
    )

    ligne = resultat.collect()[0]
    # Scission du nom complet (prénom = 1er mot, nom = 2e mot), vérifiée
    # via les hashes : AP_NOM et AP_PRENOM sont hachés puis supprimés,
    # exactement comme en mode batch
    assert ligne["Prenom_Hached"] == _sha2("Jean")
    assert ligne["Nom_Hached"] == _sha2("Dupont")
    assert ligne["FULL_NAME_Hached"] == _sha2("Jean Dupont")
    # Cast de l'identifiant
    assert resultat.schema["AP_USER_ID"].dataType.simpleString() == "int"
    # Colonnes PII d'origine supprimées, colonnes hachées présentes
    assert "AP_FULL_NAME" not in resultat.columns
    assert "AP_BIRTH_DATE" not in resultat.columns
    assert "AP_NOM" not in resultat.columns
    assert "AP_PRENOM" not in resultat.columns
    assert "Date_de_naissance_Hached" in resultat.columns


def test_appliquer_transformations_contrats_assureplus():
    """Contrats AssurePlus : prime mensuelle -> annuelle (x12, arrondie),
    casts numériques, aucune donnée PII à hacher."""
    df = spark.createDataFrame(
        [("7", "100.50", "C1")],
        "AP_USER_ID string, AP_MONTHLY_PREMIUM string, AP_CONTRACT_REF string",
    )
    resultat = _flux._appliquer_transformations(
        df, "assureplus", "contrats", salt="sel-test-local-ci"
    )

    ligne = resultat.collect()[0]
    assert ligne["AP_MONTHLY_PREMIUM"] == 1206.0  # 100.50 * 12
    assert resultat.schema["AP_USER_ID"].dataType.simpleString() == "int"
    # Aucune colonne PII hachée pour les contrats
    assert not any(
        colonne.endswith("_Hached") for colonne in resultat.columns
    )
