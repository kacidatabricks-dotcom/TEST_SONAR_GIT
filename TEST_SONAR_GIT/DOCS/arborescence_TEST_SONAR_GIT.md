# Arborescence du dossier `TEST_SONAR_GIT`

Chemin racine : `/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT`
Généré le 2026-11-20 (les internes du dossier `.git/` ne sont pas détaillés)

```text
TEST_SONAR_GIT/
├── .git/                                  (dossier interne Git — non détaillé)
├── .github/
│   └── workflows/
│       └── sonar.yml
├── README.md
├── .coveragerc
├── pytest.ini
├── requirements.txt
├── sonar-project.properties
└── TEST_SONAR_GIT/
    ├── .gitignore
    ├── Image 1.png
    ├── manifest.mf
    ├── requirements.txt
    ├── sonar-project.properties
    ├── DASHBOARD/
    │   └── Dashboard_Projet_Big_Data_STUDI.lvdash.json
    ├── DATA/
    │   └── SAMPLE/
    │       ├── ABASSURANCE/
    │       │   ├── clients.csv
    │       │   ├── AB_clients_MALFORMED_BACKUP.csv
    │       │   ├── contrats.csv
    │       │   ├── contrats.csv_CONFLIT_BACKUP.csv
    │       │   ├── sinistres.csv
    │       │   └── payments.csv
    │       └── APASSURANCE/
    │           ├── Clients.csv
    │           ├── Contrats.csv
    │           ├── Sinistres.csv
    │           └── Payments.csv
    ├── DOCS/
    │   ├── Architecture Databricks Free Edition.jpg
    │   ├── architecture.md
    │   ├── arborescence_TEST_SONAR_GIT.md
    │   ├── bug.md
    │   ├── data_mapping.md
    │   ├── data_quality.md
    │   ├── git_strategy.md
    │   ├── mcd.md
    │   └── testing.md
    ├── NOTEBOOKS/
    │   ├── .coverage
    │   ├── Notebook_Projet_Big_Data_Assurance        (notebook Databricks)
    │   ├── Notbook_Projet_Big_Data_Assurance_ANCIEN (notebook Databricks)
    │   └── TEST_PAth                                 (notebook Databricks)
    ├── SRC/
    │   ├── __init__.py.py
    │   ├── OR__init__.py
    │   ├── sonar-fonc-enregistrement-en-bronze.py
    │   ├── sonar-fonc-enregistrement-en-silver.py
    │   ├── sonar-fonc-garder-la-trace.py
    │   ├── sonar-fonc-gold-client.py
    │   ├── sonar-fonc-harmonisation-et-unification-clients.py
    │   ├── sonar-fonc-ingestion-des-donnees-assureplus.py
    │   ├── sonar-fonc-ingestion-des-donnees-oracle.py
    │   ├── sonar-fonc-matrice-doublons.py
    │   ├── sonar-fonc-nettoyage-des-deux-bases.py
    │   ├── sonar-fonc-protection-contrats-sinistres-payments.py
    │   ├── sonar-fonc-suppression-incoherences.py
    │   ├── sonar-fonc-unification-et-gold-contrat.py
    │   ├── sonar-fonc-unification-et-gold-payment.py
    │   └── sonar-fonc-unification-et-gold-sinistre.py
    └── TESTS/
        ├── __init__.py.py
        ├── conftest.py
        ├── sonar-test-enregistrement-en-bronze.py
        ├── sonar-test-enregistrement-en-silver.py
        ├── sonar-test-garder-la-trace.py
        ├── sonar-test-gold-client.py
        ├── sonar-test-harmonisation-et-unification-clients.py
        ├── sonar-test-ingestion-des-donnees-assureplus.py
        ├── sonar-test-ingestion-des-donnees-oracle.py
        ├── sonar-test-matrice-doublons.py
        ├── sonar-test-nettoyage-des-deux-bases.py
        ├── sonar-test-protection-contrats-sinistres-payments.py
        ├── sonar-test-suppression-incoherences.py
        ├── sonar-test-unification-et-gold-contrat.py
        ├── sonar-test-unification-et-gold-payment.py
        └── sonar-test-unification-et-gold-sinistre.py
```

## Résumé

| Élément | Quantité |
| --- | --- |
| Modules SRC (`sonar-fonc-*.py`) | 14 |
| Fichiers de tests (`sonar-test-*.py`) | 14 |
| Notebooks | 3 |
| Dashboards (`.lvdash.json`) | 1 |
| Documents (DOCS) | 9 |
| Échantillons de données (DATA/SAMPLE) | 10 fichiers CSV (6 AB + 4 AP) |
