#Voici un script PySpark complet à exécuter dans un notebook Databricks pour créer un DataFrame de démonstration contenant l'ensemble de vos champs avec des données fictives réalistes.

# ```python
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType, TimestampType
)
from datetime import datetime, date

# 1. Définition du schéma strict
schema = StructType([
    StructField("client_id", IntegerType(), False),
    StructField("source_system", StringType(), True),
    StructField("source_client_id", StringType(), True),
    StructField("nom", StringType(), True),
    StructField("prenom", StringType(), True),
    StructField("nom_complet", StringType(), True),
    StructField("date_naissance", DateType(), True),
    StructField("email", StringType(), True),
    StructField("telephone", StringType(), True),
    StructField("adresse", StringType(), True),
    StructField("code_postal", StringType(), True),
    StructField("numero_fiscal", StringType(), True),
    StructField("date_creation", TimestampType(), True),
    StructField("statut_client", StringType(), True),
    StructField("score_fidelite", IntegerType(), True),
    StructField("date_integration", TimestampType(), True)
])

# 2. Données de test
donnees = [
    (
        1001,
        "CRM_SALESFORCE",
        "SF-98231",
        "Dupont",
        "Jean",
        "Jean Dupont",
        date(1985, 4, 12),
        "jean.dupont@email.com",
        "+33612345678",
        "12 Rue de la Paix, Paris",
        "75002",
        "1850475001002",
        datetime(2021, 3, 15, 10, 30, 0),
        "ACTIF",
        850,
        datetime(2023, 10, 1, 8, 0, 0)
    ),
    (
        1002,
        "ERP_SAP",
        "SAP-44109",
        "Martin",
        "Sophie",
        "Sophie Martin",
        date(1992, 11, 23),
        "sophie.martin@email.com",
        "+33698765432",
        "45 Avenue Jean Jaurès, Lyon",
        "69007",
        "2921169002003",
        datetime(2022, 7, 20, 14, 15, 0),
        "ACTIF",
        420,
        datetime(2023, 10, 1, 8, 0, 0)
    ),
    (
        1003,
        "WEB_CORE",
        "WEB-00912",
        "Bernard",
        "Lucas",
        "Lucas Bernard",
        date(1978, 8, 5),
        "lucas.bernard@email.com",
        "+33711223344",
        "8 Boulevard Victor Hugo, Marseille",
        "13008",
        "1780813003004",
        datetime(2020, 1, 10, 9, 0, 0),
        "INACTIF",
        150,
        datetime(2023, 10, 1, 8, 0, 0)
    )
]

# 3. Création du DataFrame
df_clients = spark.createDataFrame(data=donnees, schema=schema)

# 4. Affichage des résultats dans Databricks
display(df_clients)



### Points clés du script :
"""
* **Formatage des types de données :**
* `client_id` et `score_fidelite` sont en `IntegerType`.
* `date_naissance` est typé en `DateType` (`YYYY-MM-DD`).
* `date_creation` et `date_integration` sont typés en `TimestampType` avec date et heure.


* **Affichage Databricks :** La commande `display(df_clients)` permet d'utiliser l'interface visuelle native de Databricks (tri, filtres, graphiques, export).
"""
