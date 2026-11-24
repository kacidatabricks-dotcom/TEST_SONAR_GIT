"""
Module : sonar-fonc-ingestion-flux
Ingestion en flux simulé (Option A — Spark Structured Streaming)
=================================================================

Transforme l'ingestion par lots (lecture de CSV) en un flux simulé :

1. `preparer_blocs_flux` découpe chaque CSV source (clients, contrats,
   sinistres, payments des deux systèmes AbAssurance/Oracle et
   AssurePlus/SQL Server) en blocs-fichiers de `taille_bloc` lignes,
   stockés dans le volume Unity Catalog `workspace.bronze.flux`.
2. `demarrer_ingestion_flux` démarre un flux Structured Streaming par
   table : `spark.readStream` + `maxFilesPerTrigger=1` +
   `trigger(availableNow=True)` => UN bloc ingéré par micro-batch,
   jusqu'à épuisement des blocs, puis le flux se termine proprement
   tout seul. (Sur compute serverless, le trigger périodique
   ProcessingTime n'est pas disponible ; AvailableNow est le trigger
   de drainage recommandé pour une source finie.) Le hachage PII
   (identique à `protection_bronze` du notebook) est appliqué avant
   l'écriture en mode append dans les tables bronze `*_flux`, avec
   checkpoint (garantie exactly-once).
3. `attendre_fin_flux` suit la progression, attend la fin des flux et
   vérifie que chaque table cible contient bien le nombre de lignes
   attendu.
4. `reinitialiser_flux` tronque les tables cibles et purge les
   checkpoints afin de rejouer la démo à l'identique.
"""

import csv as _csv
import shutil
import time
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_VOLUME_FLUX = "/Volumes/workspace/bronze/flux"
_SCHEMA_BRONZE = "workspace.bronze"
_DOSSIER_BASE = Path(__file__).resolve().parents[1] / "DOCS" / "DATA" / "SAMPLE"

# Sources : dossier de données et préfixe des tables bronze cibles
SOURCES = {
    "oracle": {"dossier": "ABASSURANCE", "prefixe_table": "oracle"},
    "assureplus": {"dossier": "APASSURANCE", "prefixe_table": "sqlserver"},
}

ENTITES = ("clients", "contrats", "sinistres", "payments")

# Types numériques attendus par colonne (mêmes types que l'ingestion par lots).
# Les colonnes lues sont toutes des chaînes (schéma généré depuis l'en-tête
# du CSV) puis castées ici ; les autres colonnes restent des chaînes.
_TYPES_NUMERIQUES = {
    ("oracle", "clients"): {"AB_CLIENT_ID": "int"},
    ("oracle", "contrats"): {
        "AB_CLIENT_ID": "int",
        "AB_PRIME_ANNUELLE": "double",
        "AB_AGENCE_ID": "int",
    },
    ("oracle", "sinistres"): {"AB_CLAIM_ID": "int", "AB_MONTANT_ESTIME": "double"},
    ("oracle", "payments"): {"AB_PAYMENT_ID": "int", "AB_MONTANT": "double"},
    ("assureplus", "clients"): {"AP_USER_ID": "int"},
    ("assureplus", "contrats"): {
        "AP_USER_ID": "int",
        "AP_MONTHLY_PREMIUM": "double",
        "AP_BROKER_CODE": "int",
    },
    ("assureplus", "sinistres"): {
        "AP_SINISTRE_NUM": "int",
        "AP_ESTIMATED_AMOUNT": "double",
    },
    ("assureplus", "payments"): {
        "AP_PAYMENT_REF": "int",
        "AP_AMOUNT_PAID": "double",
    },
}

# Colonnes PII hachées avant l'écriture en bronze (identique à
# protection_bronze du notebook : clients des deux systèmes)
_PII_HACHAGE = {
    "oracle": {
        "Email_Hached": "AB_EMAIL",
        "Nom_Hached": "AB_NOM",
        "Prenom_Hached": "AB_PRENOM",
        "Adresse_Hached": "AB_ADRESSE",
        "Telephone_Hached": "AB_TELEPHONE",
        "Num_fiscal_Hached": "AB_NUM_FISCAL",
        "Date_de_naissance_Hached": "AB_DATE_NAISSANCE",
    },
    "assureplus": {
        "Email_Hached": "AP_MAIL_ADDRESS",
        "FULL_NAME_Hached": "AP_FULL_NAME",
        "Nom_Hached": "AP_NOM",
        "Prenom_Hached": "AP_PRENOM",
        "Telephone_Hached": "AP_PHONE_NUMBER",
        "Street_Hached": "AP_STREET_ADDRESS",
        "Date_de_naissance_Hached": "AP_BIRTH_DATE",
    },
}

_PII_SUPPRIMEES = {
    "oracle": [
        "AB_NOM",
        "AB_PRENOM",
        "AB_EMAIL",
        "AB_DATE_NAISSANCE",
        "AB_ADRESSE",
        "AB_NUM_FISCAL",
        "AB_TELEPHONE",
    ],
    "assureplus": [
        "AP_FULL_NAME",
        "AP_MAIL_ADDRESS",
        "AP_BIRTH_DATE",
        "AP_NOM",
        "AP_PRENOM",
        "AP_PHONE_NUMBER",
        "AP_STREET_ADDRESS",
    ],
}


# ---------------------------------------------------------------------------
# Étape 1 : découpage des CSV en blocs-fichiers
# ---------------------------------------------------------------------------


def _trouver_csv(dossier_source, entite):
    """Retourne le chemin du CSV d'une entité (recherche insensible à la casse)."""
    dossier = Path(dossier_source)
    for candidat in sorted(dossier.glob("*.csv")):
        if candidat.stem.lower() == entite:
            return candidat
    raise FileNotFoundError(
        f"Aucun fichier CSV '{entite}.csv' trouvé dans {dossier}"
    )


def _ecrire_bloc(dossier, index, entetes, lignes):
    """Écrit un bloc-fichier CSV (l'en-tête est répété dans chaque bloc)."""
    chemin = dossier / f"bloc_{index:03d}.csv"
    with open(chemin, "w", encoding="utf-8", newline="") as sortie:
        sortie.write(entetes)
        sortie.writelines(lignes)


def decouper_csv_en_blocs(chemin_csv, dossier_sortie, taille_bloc=1000):
    """
    Découpe un fichier CSV en blocs-fichiers de `taille_bloc` lignes de
    données (l'en-tête est répété dans chaque bloc afin que chaque fichier
    soit lisible indépendamment).

    Retourne le nombre de lignes de données ingérées au total.
    """
    chemin_csv = Path(chemin_csv)
    dossier_sortie = Path(dossier_sortie)
    dossier_sortie.mkdir(parents=True, exist_ok=True)

    # Purge des anciens blocs pour éviter toute double ingestion
    for ancien in dossier_sortie.glob("bloc_*.csv"):
        ancien.unlink()

    nb_lignes = 0
    index_bloc = 0
    bloc_courant = []

    with open(chemin_csv, encoding="utf-8-sig", newline="") as source:
        entetes = source.readline()
        for ligne in source:
            if ligne.strip() == "":
                continue
            bloc_courant.append(ligne if ligne.endswith("\n") else ligne + "\n")
            nb_lignes += 1
            if len(bloc_courant) >= taille_bloc:
                _ecrire_bloc(dossier_sortie, index_bloc, entetes, bloc_courant)
                index_bloc += 1
                bloc_courant = []

    if bloc_courant:
        _ecrire_bloc(dossier_sortie, index_bloc, entetes, bloc_courant)

    if nb_lignes == 0:
        raise ValueError(f"Le fichier {chemin_csv} ne contient aucune donnée.")

    return nb_lignes


def _schema_texte(chemin_csv):
    """
    Construit un schéma tout-chaîne depuis l'en-tête réel du CSV.
    Structured Streaming exige un schéma explicite : on ne peut pas
    utiliser inferSchema. L'ordre des colonnes est celui du fichier, la
    conversion numérique est faite par `_appliquer_transformations`.
    """
    with open(chemin_csv, encoding="utf-8-sig", newline="") as fh:
        colonnes = next(_csv.reader(fh))
    return StructType(
        [StructField(colonne, StringType(), True) for colonne in colonnes]
    )


def preparer_blocs_flux(
    taille_bloc=1000,
    sources=None,
    volume_flux=_VOLUME_FLUX,
    dossier_base=_DOSSIER_BASE,
):
    """
    Découpe les CSV des sources en blocs-fichiers et prépare la
    configuration des flux.

    Retourne un dictionnaire {(source, entite): {table, dossier_blocs,
    checkpoint, schema, attendu}} où `attendu` est le nombre de lignes
    de données que le flux doit ingérer pour cette table.
    """
    config = {}
    for source, infos in SOURCES.items():
        if sources is not None and source not in sources:
            continue
        dossier_source = Path(dossier_base) / infos["dossier"]
        if not dossier_source.is_dir():
            raise FileNotFoundError(
                f"Dossier source introuvable : {dossier_source}"
            )
        for entite in ENTITES:
            chemin_csv = _trouver_csv(dossier_source, entite)
            dossier_blocs = Path(volume_flux) / source / entite
            attendu = decouper_csv_en_blocs(chemin_csv, dossier_blocs, taille_bloc)
            config[(source, entite)] = {
                "table": f"{_SCHEMA_BRONZE}.{infos['prefixe_table']}_{entite}_flux",
                "dossier_blocs": dossier_blocs.as_posix(),
                "checkpoint": (
                    Path(volume_flux) / "_checkpoints" / f"{source}_{entite}"
                ).as_posix(),
                "schema": _schema_texte(chemin_csv),
                "attendu": attendu,
            }
            print(
                f"[{source}/{entite}] {attendu} lignes découpées en blocs de "
                f"{taille_bloc} -> {dossier_blocs}"
            )
    return config


# ---------------------------------------------------------------------------
# Étape 2 : transformations (typage, règles métier, hachage PII)
# ---------------------------------------------------------------------------


def _hacher_pii_clients(df, source, salt):
    """Hache puis supprime les colonnes PII des clients (identique à
    protection_bronze du notebook)."""
    sel = F.lit(salt)
    for cible, origine in _PII_HACHAGE[source].items():
        if origine in df.columns:
            df = df.withColumn(
                cible, F.sha2(F.concat(F.col(origine), sel), 256)
            )
    a_supprimer = [c for c in _PII_SUPPRIMEES[source] if c in df.columns]
    return df.drop(*a_supprimer) if a_supprimer else df


def _appliquer_transformations(df, source, entite, salt):
    """Applique les mêmes règles que l'ingestion par lots (typage,
    scission du nom complet AssurePlus, prime mensuelle -> annuelle,
    hachage PII des clients)."""

    # 1. Typage des colonnes numériques
    for colonne, typage in _TYPES_NUMERIQUES.get((source, entite), {}).items():
        if colonne in df.columns:
            df = df.withColumn(colonne, F.col(colonne).cast(typage))

    # 2. Règles métier identiques au mode batch
    if source == "assureplus" and entite == "clients":
        # Scinder AP_FULL_NAME en nom et prénom (même règle que le batch)
        df = df.withColumn(
            "AP_NOM", F.trim(F.split(F.col("AP_FULL_NAME"), " ").getItem(1))
        ).withColumn(
            "AP_PRENOM", F.trim(F.split(F.col("AP_FULL_NAME"), " ").getItem(0))
        )
    if source == "assureplus" and entite == "contrats":
        # Prime mensuelle -> annuelle (même règle que le batch)
        df = df.withColumn(
            "AP_MONTHLY_PREMIUM",
            F.round(F.col("AP_MONTHLY_PREMIUM").cast("double") * 12, 2),
        )

    # 3. Hachage PII des clients
    if entite == "clients":
        df = _hacher_pii_clients(df, source, salt)

    return df


# ---------------------------------------------------------------------------
# Étape 3 : démarrage des flux Structured Streaming
# ---------------------------------------------------------------------------


def demarrer_ingestion_flux(spark, config, salt):
    """
    Démarre un flux Structured Streaming par table cible.

    Chaque flux lit les blocs-fichiers de son dossier avec
    `maxFilesPerTrigger=1` : un bloc est ingéré par micro-batch, jusqu'à
    épuisement des blocs (le trigger AvailableNow termine le flux
    proprement dès que tous les fichiers disponibles ont été traités).
    Le résultat est écrit en mode append dans la table bronze `*_flux`,
    avec colonnes de traçabilité `_micro_batch` et
    `_horodatage_ingestion`.

    Retourne {cle_config: StreamingQuery}.
    """
    requetes = {}
    for (source, entite), infos in config.items():

        def _ecrire_micro_batch(batch_df, batch_id, table=infos["table"]):
            (batch_df.withColumn("_micro_batch", F.lit(batch_id))
                .withColumn("_horodatage_ingestion", F.current_timestamp())
                .write.mode("append")
                .saveAsTable(table))

        df_flux = (
            spark.readStream.format("csv")
            .option("header", True)
            .schema(infos["schema"])
            .option("maxFilesPerTrigger", 1)
            .load(infos["dossier_blocs"])
        )
        df_flux = _appliquer_transformations(df_flux, source, entite, salt)

        requete = (
            df_flux.writeStream
            .foreachBatch(_ecrire_micro_batch)
            .option("checkpointLocation", infos["checkpoint"])
            .trigger(availableNow=True)
            .queryName(f"flux_{source}_{entite}")
            .start()
        )
        requetes[(source, entite)] = requete
        print(
            f"[{source}/{entite}] flux démarré -> {infos['table']} "
            "(un bloc par micro-batch, jusqu'à épuisement des blocs)"
        )
    return requetes


# ---------------------------------------------------------------------------
# Étape 4 : attente de fin et vérification
# ---------------------------------------------------------------------------


def attendre_fin_flux(spark, requetes, config, delai_max_secondes=600, pas_secondes=5):
    """
    Attend la fin des flux (le trigger AvailableNow les termine dès
    que tous les blocs disponibles ont été ingérés), suit la
    progression des tables cibles, puis vérifie que chaque table
    contient bien le nombre de lignes attendu.

    Retourne True si toutes les tables ont atteint leur effectif attendu.
    """
    attendus = {infos["table"]: infos["attendu"] for infos in config.values()}
    debut = time.time()

    # Attente active : les flux s'arrêtent d'eux-mêmes une fois épuisés
    while any(requete.isActive for requete in requetes.values()):
        if time.time() - debut > delai_max_secondes:
            for requete in requetes.values():
                if requete.isActive:
                    requete.stop()
            print("Délai maximal atteint — flux arrêtés (ingestion incomplète).")
            return False
        time.sleep(pas_secondes)
        progression = []
        for table, attendu in attendus.items():
            nb = spark.table(table).count()
            progression.append(f"{table.split('.')[-1]}: {nb}/{attendu}")
        print(" | ".join(progression))

    for requete in requetes.values():
        requete.awaitTermination(timeout=60)

    # Vérification finale
    conforme = True
    for table, attendu in attendus.items():
        nb = spark.table(table).count()
        print(f"{table}: {nb}/{attendu}")
        if nb != attendu:
            conforme = False
    if conforme:
        print("Tous les blocs ont été ingérés — flux terminés proprement.")
    else:
        print("Incohérence détectée : certaines tables sont incomplètes.")
    return conforme


def reinitialiser_flux(spark, config):
    """
    Tronque les tables cibles et purge les checkpoints afin de pouvoir
    rejouer la démo du flux sans doublons.
    """
    for infos in config.values():
        try:
            spark.sql(f"TRUNCATE TABLE {infos['table']}")
        except Exception:
            # La table n'existe pas encore au premier lancement
            pass
        checkpoint = Path(infos["checkpoint"])
        if checkpoint.exists():
            shutil.rmtree(checkpoint)
    print("Tables cibles tronquées et checkpoints purgés.")
