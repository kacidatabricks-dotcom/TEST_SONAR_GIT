# Tests du module sonar-fonc-ingestion-flux (ingestion en flux simulé).
#
# Fonctions testées : decouper_csv_en_blocs, _trouver_csv, _schema_texte,
# _appliquer_transformations (typage numérique, règles métier, hachage PII),
# preparer_blocs_flux, attendre_fin_flux, reinitialiser_flux et
# demarrer_ingestion_flux (intégration streaming de bout en bout).
# Ces tests sont portables : fichiers CSV créés dans des dossiers
# temporaires, sel de hachage constant — aucune dépendance aux vraies
# données. Les tables créées le sont dans le schéma Spark par défaut du
# runner CI (jamais workspace.bronze.*_flux de production).
# L'intégration streaming complète (test_demarrer_et_attendre_flux)
# requiert que Spark lise le système de fichiers local du runner : sur un
# notebook Databricks serverless (Spark Connect), elle est déléguée au CI
# et à la cellule « Ingestion en flux simulé » du notebook.

import hashlib
import importlib
import tempfile
from pathlib import Path

from pyspark.sql import functions as F

_flux = importlib.import_module("sonar-fonc-ingestion-flux")


def _spark_est_connect(spark):
    """True si la session Spark est un client Spark Connect (notebook
    Databricks serverless) : le moteur distant ne peut pas lire les
    chemins locaux du driver, l'intégration streaming complète est alors
    laissée au CI (Spark local) et à la cellule flux du notebook."""
    try:
        from pyspark.sql.connect.session import SparkSession as SessionConnect
    except ImportError:  # pyspark < 3.4 : pas de Spark Connect du tout
        return False
    return isinstance(spark, SessionConnect)


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


def test_decouper_csv_vide_leve_erreur():
    """Un CSV sans aucune ligne de données lève une ValueError : on ne
    démarre pas silencieusement un flux qui n'ingérerait rien."""
    with tempfile.TemporaryDirectory() as dossier:
        source = _ecrire_csv_de_test(dossier, "clients.csv", ["ID", "NOM"], [])
        try:
            _flux.decouper_csv_en_blocs(source, Path(dossier) / "blocs")
        except ValueError:
            pass
        else:
            raise AssertionError("ValueError attendue pour un CSV vide")


def test_decouper_csv_normalise_derniere_ligne():
    """Une dernière ligne sans retour à la chariot et des lignes vides
    parasites sont normalisées : chaque bloc reste un CSV bien formé."""
    with tempfile.TemporaryDirectory() as dossier:
        chemin = Path(dossier) / "clients.csv"
        # Dernière ligne sans "\n" final + une ligne vide parasite
        chemin.write_text(
            "ID,NOM\n1,Martin\n\n2,Durand", encoding="utf-8"
        )
        nb_lignes = _flux.decouper_csv_en_blocs(
            chemin, Path(dossier) / "blocs", taille_bloc=1
        )
        assert nb_lignes == 2  # la ligne vide n'est pas comptée
        blocs = sorted((Path(dossier) / "blocs").glob("bloc_*.csv"))
        assert [b.name for b in blocs] == ["bloc_000.csv", "bloc_001.csv"]
        # Chaque bloc se termine par un retour à la ligne normalisé
        assert blocs[1].read_text(encoding="utf-8") == "ID,NOM\n2,Durand\n"


def test_preparer_blocs_flux():
    """preparer_blocs_flux découpe les 8 CSV sources (2 systèmes x 4
    entités) et construit la configuration complète des flux : nom de
    table bronze, dossier de blocs, checkpoint, schéma et effectif
    attendu."""
    with tempfile.TemporaryDirectory() as base, tempfile.TemporaryDirectory() as volume:
        for dossier_source in ("ABASSURANCE", "APASSURANCE"):
            for entite in _flux.ENTITES:
                dossier = Path(base) / dossier_source
                dossier.mkdir(parents=True, exist_ok=True)
                _ecrire_csv_de_test(
                    dossier,
                    f"{entite}.csv",
                    ["ID", "NOM"],
                    [[str(i), f"nom{i}"] for i in range(1, 4)],
                )

        config = _flux.preparer_blocs_flux(
            taille_bloc=2, volume_flux=volume, dossier_base=base
        )

        # 8 flux configurés : oracle->oracle_*, assureplus->sqlserver_*
        assert set(config) == {
            (source, entite)
            for source in ("oracle", "assureplus")
            for entite in _flux.ENTITES
        }
        infos = config[("oracle", "clients")]
        assert infos["table"] == "workspace.bronze.oracle_clients_flux"
        assert config[("assureplus", "clients")]["table"] == (
            "workspace.bronze.sqlserver_clients_flux"
        )

        # Blocs réellement écrits : 3 lignes en blocs de 2 -> 2 blocs
        assert infos["attendu"] == 3
        dossier_blocs = Path(infos["dossier_blocs"])
        assert dossier_blocs == Path(volume) / "oracle" / "clients"
        assert len(list(dossier_blocs.glob("bloc_*.csv"))) == 2

        # Checkpoint dédié par flux, schéma tout-chaîne depuis l'en-tête
        assert infos["checkpoint"] == (
            (Path(volume) / "_checkpoints" / "oracle_clients").as_posix()
        )
        assert [c.name for c in infos["schema"].fields] == ["ID", "NOM"]
        assert all(
            c.dataType.simpleString() == "string"
            for c in infos["schema"].fields
        )


def test_preparer_blocs_flux_filtre_source():
    """Le paramètre sources restreint la préparation : seuls les flux
    demandés sont configurés (les dossiers des autres sources ne sont
    même pas consultés)."""
    with tempfile.TemporaryDirectory() as base, tempfile.TemporaryDirectory() as volume:
        # Seul ABASSURANCE existe : le filtre évite l'erreur sur APASSURANCE
        dossier = Path(base) / "ABASSURANCE"
        dossier.mkdir(parents=True)
        for entite in _flux.ENTITES:
            _ecrire_csv_de_test(dossier, f"{entite}.csv", ["ID"], [["1"]])

        config = _flux.preparer_blocs_flux(
            taille_bloc=100,
            sources=["oracle"],
            volume_flux=volume,
            dossier_base=base,
        )
        assert set(config) == {
            ("oracle", entite) for entite in _flux.ENTITES
        }


def test_preparer_blocs_flux_dossier_absent():
    """Un dossier source introuvable lève une FileNotFoundError explicite
    plutôt qu'un flux démarré sur rien."""
    with tempfile.TemporaryDirectory() as base, tempfile.TemporaryDirectory() as volume:
        try:
            _flux.preparer_blocs_flux(
                volume_flux=volume, dossier_base=base
            )
        except FileNotFoundError:
            pass
        else:
            raise AssertionError("FileNotFoundError attendue")


class _RequeteFictive:
    """Requête de streaming factive pour piloter attendre_fin_flux sans
    démarrer de vrai flux : état actif pilotable, arrêt enregistré."""

    def __init__(self, active):
        self._active = active
        self.arretee = False

    @property
    def isActive(self):
        return self._active

    def stop(self):
        self.arretee = True
        self._active = False

    def awaitTermination(self, timeout=None):
        return True


def test_attendre_fin_flux_conforme_puis_incoherent():
    """Une fois les flux terminés, la vérification compare l'effectif
    réel de chaque table cible à l'attendu : True si conforme, False si
    une table est incomplète (sans jamais arrêter une requête finie)."""
    spark.range(3).write.mode("overwrite").saveAsTable(
        "sonar_ci_attendre_fin"
    )
    try:
        requete = _RequeteFictive(False)  # flux déjà terminé
        config_ok = {
            ("oracle", "clients"): {
                "table": "sonar_ci_attendre_fin",
                "attendu": 3,
            }
        }
        assert _flux.attendre_fin_flux(
            spark, {"q": requete}, config_ok, delai_max_secondes=5
        ) is True
        assert requete.arretee is False

        config_ko = {
            ("oracle", "clients"): {
                "table": "sonar_ci_attendre_fin",
                "attendu": 4,
            }
        }
        assert _flux.attendre_fin_flux(
            spark, {"q": _RequeteFictive(False)}, config_ko, delai_max_secondes=5
        ) is False
    finally:
        spark.sql("DROP TABLE IF EXISTS sonar_ci_attendre_fin")


def test_attendre_fin_flux_delai_depasse():
    """Si un flux reste actif au-delà du délai maximal, les requêtes
    sont arrêtées et la fonction retourne False (ingestion incomplète)."""
    requete = _RequeteFictive(True)  # flux bloqué
    config = {
        ("oracle", "clients"): {
            "table": "sonar_ci_attendre_timeout",
            "attendu": 1,
        }
    }
    assert _flux.attendre_fin_flux(
        spark, {"q": requete}, config, delai_max_secondes=0, pas_secondes=1
    ) is False
    assert requete.arretee is True


def test_reinitialiser_flux():
    """reinitialiser_flux tronque les tables cibles (même absentes : le
    premier lancement ne doit pas échouer) et purge les checkpoints."""
    spark.range(2).write.mode("overwrite").saveAsTable("sonar_ci_reinit")
    with tempfile.TemporaryDirectory() as dossier:
        checkpoint = Path(dossier) / "ckpt"
        (checkpoint / "offsets").mkdir(parents=True)
        config = {
            ("oracle", "clients"): {
                "table": "sonar_ci_reinit",
                "checkpoint": checkpoint.as_posix(),
            },
            ("assureplus", "clients"): {
                # Table jamais créée : l'exception de TRUNCATE est ignorée
                "table": "sonar_ci_reinit_absente",
                "checkpoint": (Path(dossier) / "absent").as_posix(),
            },
        }
        _flux.reinitialiser_flux(spark, config)
        assert spark.table("sonar_ci_reinit").count() == 0
        assert not checkpoint.exists()
    spark.sql("DROP TABLE IF EXISTS sonar_ci_reinit")


def test_demarrer_et_attendre_flux():
    """Intégration streaming de bout en bout sur données minuscules :
    découpage en 2 blocs -> demarrer_ingestion_flux (un bloc par
    micro-batch, trigger AvailableNow) -> attendre_fin_flux conforme,
    colonnes de traçabilité et hachage PII vérifiés dans la table
    cible."""
    if _spark_est_connect(spark):
        # Spark Connect (notebook Databricks) : le moteur distant ne lit
        # pas le dossier local du driver — validé par le CI Spark local
        # et par la cellule « Ingestion en flux simulé » du notebook.
        return

    with tempfile.TemporaryDirectory() as dossier:
        source = _ecrire_csv_de_test(
            dossier,
            "clients.csv",
            ["AB_CLIENT_ID", "AB_NOM", "AB_EMAIL"],
            [["1", "Martin", "martin@example.fr"], ["2", "Durand", "durand@example.fr"]],
        )
        dossier_blocs = Path(dossier) / "blocs"
        assert _flux.decouper_csv_en_blocs(source, dossier_blocs, 1) == 2

        config = {
            ("oracle", "clients"): {
                "table": "sonar_ci_flux_e2e",
                "dossier_blocs": dossier_blocs.as_posix(),
                "checkpoint": (Path(dossier) / "ckpt").as_posix(),
                "schema": _flux._schema_texte(source),
                "attendu": 2,
            }
        }
        requetes = _flux.demarrer_ingestion_flux(
            spark, config, salt="sel-test-local-ci"
        )
        try:
            assert set(requetes) == {("oracle", "clients")}
            assert _flux.attendre_fin_flux(
                spark, requetes, config, delai_max_secondes=120, pas_secondes=1
            ) is True

            resultat = spark.table("sonar_ci_flux_e2e")
            assert resultat.count() == 2
            colonnes = set(resultat.columns)
            assert "_micro_batch" in colonnes
            assert "_horodatage_ingestion" in colonnes
            # Règles métier appliquées par le flux, à l'identique du batch
            assert resultat.schema["AB_CLIENT_ID"].dataType.simpleString() == "int"
            assert "Nom_Hached" in colonnes and "AB_NOM" not in colonnes
            ligne = resultat.where(F.col("AB_CLIENT_ID") == 1).collect()[0]
            assert ligne["Nom_Hached"] == _sha2("Martin")
        finally:
            for requete in requetes.values():
                if requete.isActive:
                    requete.stop()
            spark.sql("DROP TABLE IF EXISTS sonar_ci_flux_e2e")

