# Projet Big Data Assurance – Databricks Free Edition

## 1. Présentation du projet

Ce projet consiste à mettre en place une plateforme Big Data permettant de collecter, traiter, nettoyer, rapprocher et consolider les données provenant de deux systèmes d'information d'une compagnie d'assurance :

- **ABASSURANCE** : base de données relationnelle Oracle.
- **ASSUREPLUS** : base de données relationnelle SQL Server.

L'objectif est de centraliser les données issues de ces deux systèmes afin d'obtenir une vision unifiée et fiable des informations relatives aux clients, aux contrats et aux autres données métier de l'assurance.

Le projet est réalisé avec **Databricks Free Edition**, une plateforme permettant de développer et d'exécuter des traitements de données avec Apache Spark, dans un environnement cloud accessible gratuitement sous certaines limites.

## 2. Objectifs du projet

Les principaux objectifs sont :

- Collecter les données provenant des systèmes ABASSURANCE et ASSUREPLUS.
- Intégrer des données issues de sources hétérogènes.
- Nettoyer et normaliser les données.
- Identifier les doublons entre les deux systèmes.
- Rapprocher les clients présents dans les deux bases.
- Détecter les incohérences et les anomalies.
- Consolider les données dans un référentiel commun.
- Mettre à disposition des données fiables pour les analyses et les tableaux de bord.

## 3. Environnement technique

| Technologie | Utilisation |
|---|---|
| Databricks Free Edition | Plateforme de développement et de traitement Big Data |
| Apache Spark | Traitement distribué des données |
| PySpark | Développement des traitements de données |
| Python | Scripts et logique de traitement |
| Delta Lake | Stockage et gestion des tables Delta lorsque disponible |
| SQL | Requêtes et analyses de données |
| Oracle | Système source ABASSURANCE |
| SQL Server | Système source ASSUREPLUS |
| Git / GitHub | Gestion du code source et versionnement |

**Remarque :** Databricks Free Edition présente des limitations en matière de ressources, de fonctionnalités et d'exécution. L'architecture et les traitements doivent donc être adaptés aux possibilités de cet environnement. Les connexions directes à Oracle et SQL Server dépendent notamment de la disponibilité des accès et des pilotes nécessaires. Pour les premiers tests, des fichiers CSV ou Parquet peuvent être utilisés comme sources simulées.

## 4. Architecture du projet

Le projet s'appuie sur une architecture de type **Medallion Architecture**, organisée en trois couches :

```text
        SOURCES DE DONNÉES
        ------------------
        ABASSURANCE (Oracle)
        ASSUREPLUS (SQL Server)
                 |
                 v
        +-------------------+
        |   BRONZE LAYER    |
        | Données brutes    |
        | Collecte et copie |
        +-------------------+
                 |
                 v
        +-------------------+
        |   SILVER LAYER    |
        | Nettoyage         |
        | Normalisation     |
        | Déduplication     |
        | Rapprochement     |
        +-------------------+
                 |
                 v
        +-------------------+
        |    GOLD LAYER     |
        | Données consolidées|
        | Référentiel client|
        | Données métier    |
        +-------------------+
                 |
                 v
        ANALYSES ET REPORTING
```

### 4.1 Bronze Layer – Données brutes

Cette couche contient les données extraites des systèmes sources, sans transformation métier importante.

Exemples :
- Clients ABASSURANCE.
- Clients ASSUREPLUS.
- Contrats d'assurance.
- Informations relatives aux polices.
- Données de référence.

L'objectif est de conserver une copie des données sources pour garantir leur traçabilité.

### 4.2 Silver Layer – Données nettoyées et harmonisées

Cette couche contient les données après nettoyage et transformation.

Les traitements réalisés comprennent :
- Suppression ou identification des doublons.
- Gestion des valeurs nulles.
- Normalisation des noms et prénoms.
- Uniformisation des formats de dates.
- Normalisation des adresses e-mail et des numéros de téléphone.
- Harmonisation des structures de données.
- Rapprochement des clients entre ABASSURANCE et ASSUREPLUS.

### 4.3 Gold Layer – Données consolidées

Cette couche contient les données prêtes à être exploitées par les utilisateurs métier.

Exemples :
- Référentiel client consolidé.
- Liste des clients présents dans les deux systèmes.
- Liste des clients spécifiques à chaque système.
- Indicateurs de qualité des données.
- Statistiques sur les contrats d'assurance.

## 5. Fonctionnement du rapprochement des clients

Une fonctionnalité importante du projet consiste à identifier les clients qui correspondent à une même personne dans les deux systèmes.

Le rapprochement peut s'appuyer sur plusieurs critères :

| Critère | Exemple |
|---|---|
| Nom | DUPONT |
| Prénom | Jean |
| Date de naissance | 15/05/1985 |
| Adresse e-mail | jean.dupont@email.com |
| Téléphone | 0612345678 |
| Adresse postale | 10 rue de Paris |

Le principe est de comparer les informations disponibles dans les deux bases et de calculer un score de similarité.

Pour ma première version, je n'utilise pas le score de similarité. je m'appuie sur sur une match ky qui concatène le nom, le prénom et la data de naissance. La probabilité de détecter de vrais doublons est très élevée.



## 6. Organisation du dépôt

Proposition d'organisation des fichiers du projet :

```text
big-data-assurance/
│
├── README.md
│
├── data/
│   ├── raw/
│   │   ├── abassurance/
│   │   └── assureplus/
│   │
│   └── samples/
│       ├── clients_abassurance.csv
│       ├── clients_assureplus.csv
│       └── contrats.csv
│
├── notebooks/
│   ├── 01_ingestion_bronze.py
│   ├── 02_nettoyage_silver.py
│   ├── 03_normalisation.py
│   ├── 04_deduplication.py
│   ├── 05_matching_clients.py
│   ├── 06_consolidation_gold.py
│   └── 07_controle_qualite.py
│
├── sql/
│   ├── creation_tables.sql
│   └── analyses_metier.sql
│
├── documentation/
│   ├── architecture.md
│   ├── dictionnaire_donnees.md
│   └── regles_gestion.md
│
└── tests/
    ├── test_nettoyage.py
    ├── test_matching.py
    └── test_qualite.py
```

Cette organisation est une proposition : les notebooks peuvent être développés directement dans l'espace de travail Databricks, puis exportés dans le dépôt Git.

## 7. Étapes de réalisation

Le projet est organisé selon une démarche Agile, avec des développements progressifs.

### Étape 1 – Analyse des besoins
- Comprendre les systèmes ABASSURANCE et ASSUREPLUS.
- Identifier les données à intégrer.
- Définir les règles métier.
- Identifier les problèmes de qualité des données.

### Étape 2 – Mise en place de l'environnement
- Créer un compte Databricks Free Edition.
- Créer l'espace de travail.
- Préparer les notebooks Python et PySpark.
- Préparer les données de test.

### Étape 3 – Ingestion des données
- Importer les données ABASSURANCE.
- Importer les données ASSUREPLUS.
- Stocker les données brutes dans la couche Bronze.

### Étape 4 – Nettoyage et transformation
- Nettoyer les données.
- Normaliser les champs.
- Gérer les valeurs manquantes.
- Détecter les doublons.
- Alimenter la couche Silver.

### Étape 5 – Rapprochement des données
- Définir les clés de rapprochement.
- Comparer les informations clients.


### Étape 6 – Consolidation
- Construire le référentiel client.
- Consolider les données métier.
- Alimenter la couche Gold.

### Étape 7 – Tests et validation
- Vérifier la qualité des données.
- Tester les règles de rapprochement.
- Contrôler les volumes traités.
- Vérifier la cohérence des résultats.

## 8. Gestion Agile du projet

Le projet suit une organisation Agile basée sur des sprints et des User Stories.

Exemples de User Stories :

| ID | User Story | Priorité |
|---|---|---|
| US-01 | En tant que data Engineer, je veux importer les données Oracle d'AbAssurance afin de les rendre disponibles dans la plateforme analytique unifiée | Haute |
| US-02 | En tant que data Engineer, je veux importer les données SQL Server d'AssurePlus afin de les intégrer au pipeline. | Haute |
| US-03 | En tant que data Engineer, je veux normaliser les noms, Emails, téléphones, adresses, dates… afin de faciliter le rapprochement. | Haute |
| US-04 | En tant que Responsable des Sinistres, je veux un Dashboard pour suivre les sinistres (fréquence, coût moyen, délai de traitement). | Haute |


Les estimations de complexité peuvent être réalisées collectivement avec la méthode Planning Poker.

En cas de désaccord entre développeurs :
- Clarifier le périmètre de la User Story.
- Identifier les difficultés techniques.
- Décomposer les User Stories trop complexes.
- Réaliser un Spike technique si nécessaire.
- Réestimer collectivement la charge.

## 9. Lancement du projet sur Databricks Free Edition

### Prérequis

- Un compte Databricks Free Edition.
- Un navigateur Internet.
- Des fichiers de données de test au format CSV ou Parquet.
- Des connaissances de base en Python, SQL et PySpark.

### Création du projet

1. Accéder à https://www.databricks.com/learn/free-edition
2. Créer un compte et accéder à Databricks Free Edition.
3. Créer un espace de travail.
4. Créer un notebook Python.
5. Importer les fichiers de données de test.
6. Développer les traitements PySpark.
7. Vérifier les résultats dans les tables ou fichiers générés.

Dans le fichier du dépôt 1, j'ai fourni les identifiants pour accéder à mon projet.
### Exemple de lecture d'un fichier CSV avec PySpark

```python
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/catalog/schema/volume/clients_abassurance.csv")

display(df)
```

Le chemin d'accès est à adapter à l'emplacement réel du fichier dans Databricks.

### Exemple de nettoyage des données

```python
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower
)

df_clean = df \
    .dropDuplicates() \
    .withColumn("nom", upper(trim(col("nom")))) \
    .withColumn("prenom", upper(trim(col("prenom")))) \
    .withColumn("email", lower(trim(col("email"))))

display(df_clean)
```

Cet exemple suppose que les colonnes `nom`, `prenom` et `email` existent dans le fichier source.

## 10. Sécurité et qualité des données

Les données d'assurance peuvent contenir des informations personnelles. Le projet doit donc respecter les principes de protection des données personnelles, notamment le RGPD.

Mesures recommandées :
- Utiliser des données fictives ou anonymisées pour les tests.
- Ne pas déposer de données personnelles réelles dans un dépôt Git public.
- Ne jamais enregistrer de mots de passe ou de clés d'accès dans les notebooks.
- Limiter l'accès aux données.
- Contrôler la qualité et la traçabilité des traitements.
- Documenter les règles de transformation.

## 11. Résultats attendus

À la fin du projet, la plateforme devra permettre :

- D'intégrer les données de deux systèmes d'assurance différents.
- De centraliser les données dans une architecture Bronze, Silver et Gold.
- De nettoyer et harmoniser les données.
- De détecter les doublons.
- De rapprocher les clients présents dans les deux systèmes.
- De produire un référentiel client consolidé.
- De fournir des données fiables pour les analyses métier.

## 12. Limites de Databricks Free Edition

Databricks Free Edition est utilisé dans ce projet pour des raisons pédagogiques et de démonstration.

Il convient de prendre en compte :
- Les ressources de calcul limitées.
- Les restrictions sur certaines fonctionnalités avancées.
- Les limites de stockage et d'exécution.
- Les possibilités variables de connexion aux bases externes.
- L'absence de garantie de performances comparables à un environnement de production.

Pour un projet réel traitant plusieurs millions d'enregistrements, il sera nécessaire d'évaluer les ressources, les performances, la sécurité et les coûts d'une architecture Databricks adaptée à la production.

## 13. Conclusion

Ce projet permet de mettre en pratique les principales étapes d'un projet Big Data dans le secteur de l'assurance : ingestion, transformation, nettoyage, rapprochement et consolidation des données.

L'utilisation de Databricks Free Edition permet de développer et de tester une première solution Big Data avec Apache Spark et PySpark, tout en appliquant une organisation Agile.

L'architecture proposée constitue une base évolutive qui pourra être enrichie par de nouvelles sources de données, des règles métier supplémentaires et des outils de visualisation.



**Projet pédagogique – Big Data Assurance**

**Plateforme :** Databricks Free Edition  
**Traitement :** Apache Spark / PySpark  
**Architecture :** Medallion (Bronze, Silver, Gold)  
**Méthodologie :** Agile / Scrum









