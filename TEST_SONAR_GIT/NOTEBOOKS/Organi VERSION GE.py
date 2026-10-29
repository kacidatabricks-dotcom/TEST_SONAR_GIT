"""
PARTIE 3 : IMPLEMENTATION TECHNIQUE SUR DATABRICKS (Sujet 3)
Dossier 1 : Structure Git et Modèle de Données
Question 1 : Repository Git
•	Branches : main (production), develop (intégration), feature/ingestion-oracle, feature/deduplication, bugfix/date-format.

•	Convention de nommage : <type>/<ticket-id>-<description-courte> (Ex: feature/AB-102-pipeline-sinistres).

Question 2 : Modèle Conceptuel des Données (MCD Unifié)
Le modèle réconcilie la structure d'AbAssurance (Oracle) et d'AssurePlus (SQL Server) : 


+-------------------------------------------------+
|                  DIM_CLIENT                     |
+-------------------------------------------------+
| master_client_id (PK)                           |
| source_system ('ORACLE' / 'SQL_SERVER')         |
| original_id                                     |
| nom, prenom, date_naissance, email, telephone   |
| adresse, code_postal, ville, pays               |
+-------------------------------------------------+
                        │ 1
                        │
                        │ N
+-------------------------------------------------+
|                  FACT_CONTRAT                   |
+-------------------------------------------------+
| contrat_id (PK)                                 |
| master_client_id (FK)                           |
| type_contrat ('SANTE', 'AUTO', 'HABITATION'...) |
| date_debut, date_fin, statut                    |
| prime_annuelle_ttc                              |
+-------------------------------------------------+
                        │ 1
                        │
                        │ N
+-------------------------------------------------+
|                  FACT_SINISTRE                  |
+-------------------------------------------------+
| sinistre_id (PK)                                |
| contrat_id (FK)                                 |
| date_sinistre, date_declaration                 |
| type_sinistre, statut ('OUVERT', 'CLOS'...)    |
| montant_indemnise, cout_estime                  |
+-------------------------------------------------+
Dossier 2 & 3 : Implémentation complète sous Databricks (PySpark)
Vous pouvez copier-coller le code ci-dessous directement dans un Notebook Databricks (Python) sur la version gratuite (Community Edition).

"""
#Étape 1 : Simulation des jeux de données Oracle & SQL Server

from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap,  col, lower, upper, trim, concat_ws, levenshtein, when, lit, count, sum as _sum, avg, round

# 1. Données AbAssurance (Oracle Source)
oracle_clients = spark.createDataFrame([
    (101, "DUPONT", "Jean", "1980-05-12", "jean.dupont@email.fr", "0601020304", "Paris"),
    (102, "MARTIN", "Sophie", "1992-11-20", "sophie.martin@email.fr", "0611223344", "Lyon"),
    (103, "BENALI", "Amina", "1988-03-15", "a.benali@email.fr", "0655667788", "Marseille")
], ["ID_CLIENT", "NOM", "PRENOM", "DATE_NAISS", "EMAIL", "TEL", "VILLE"])

oracle_contrats = spark.createDataFrame([
    (5001, 101, "AUTO", "2021-01-01", "ACTIF", 850.0),
    (5002, 102, "HABITATION", "2020-06-15", "ACTIF", 450.0),
    (5003, 103, "SANTE", "2019-03-01", "ACTIF", 1200.0)
], ["ID_CONTRAT", "ID_CLIENT", "TYPE_CONTRAT", "DATE_DEBUT", "STATUT", "PRIME"])

oracle_sinistres = spark.createDataFrame([
    (9001, 5001, "2023-04-10", "ACCIDENT", "CLOS", 1500.0),
    (9002, 5002, "2023-08-12", "DEGAT_EAUX", "EN_COURS", 800.0)
], ["ID_SINISTRE", "ID_CONTRAT", "DATE_SINISTRE", "TYPE", "STATUT", "MONTANT"])

# 2. Données AssurePlus (SQL Server Source)
sqlserver_customers = spark.createDataFrame([
    ("C-201", "Dupont", "Jean", "1980-05-12", "j.dupont@email.fr", "0601020304", "Paris"), # Doublon potentiel avec Jean Dupont
    ("C-202", "GIRARD", "Lucas", "1995-07-08", "lucas.g@email.fr", "0788990011", "Toulouse")
], ["CustomerID", "LastName", "FirstName", "BirthDate", "EmailAddress", "Phone", "City"])

sqlserver_policies = spark.createDataFrame([
    ("P-701", "C-201", "AUTO_PREMIUM", "2022-03-10", "ACTIVE", 1100.0),
    ("P-702", "C-202", "AUTO_STANDARD", "2023-01-15", "ACTIVE", 650.0)
], ["PolicyID", "CustomerID", "PolicyType", "StartDate", "Status", "Premium"])

sqlserver_claims = spark.createDataFrame([
    ("CL-301", "P-701", "2023-11-02", "COLLISION", "CLOSED", 2100.0)
], ["ClaimID", "PolicyID", "ClaimDate", "ClaimType", "Status", "Amount"])

# Étape 2 : Data Pipeline - Harmonisation & Unification (Silver Layer)

# Harmonisation des clients Oracle
std_oracle_cli = oracle_clients.select(
    lit("ORACLE").alias("source_system"),
    col("ID_CLIENT").cast("string").alias("original_id"),
    upper(trim(col("NOM"))).alias("nom"),
    initcap(trim(col("PRENOM"))).alias("prenom"),
    col("DATE_NAISS").alias("date_naissance"),
    lower(trim(col("EMAIL"))).alias("email"),
    col("TEL").alias("telephone"),
    col("VILLE").alias("ville")
)

# Harmonisation des clients SQL Server
std_sql_cli = sqlserver_customers.select(
    lit("SQL_SERVER").alias("source_system"),
    col("CustomerID").alias("original_id"),
    upper(trim(col("LastName"))).alias("nom"),
    initcap(trim(col("FirstName"))).alias("prenom"),
    col("BirthDate").alias("date_naissance"),
    lower(trim(col("EmailAddress"))).alias("email"),
    col("Phone").alias("telephone"),
    col("City").alias("ville")
)

# Union des données
all_clients = std_oracle_cli.union(std_sql_cli)
all_clients.createOrReplaceTempView("raw_unified_clients")
all_clients.display()
# Étape 3 : Matrice de Doublons et Création de la Dimension Client

# Algorithme d'identification des doublons (Matching exact sur Nom, Prénom, Date de Naissance ou Téléphone)
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, upper, initcap

# Génération d'une clé de rapprochement
matched_clients = all_clients.withColumn(
    "match_key", 
    concat_ws("_", col("nom"), col("prenom"), col("date_naissance"))
)

# Attribution d'un ID Master Unique pour les doublons
windowSpec = Window.orderBy("match_key")
dim_client = matched_clients.withColumn("master_client_id", dense_rank().over(windowSpec))

dim_client.write.format("delta").mode("overwrite").saveAsTable("gold_dim_client")

# Affichage de la Matrice de Doublons
print("--- MATRICE DE DOUBLONS IDENTIFIÉS ---")
doublons_df = dim_client.groupBy("master_client_id") \
    .agg(count("original_id").alias("nb_occurrences")) \
    .filter(col("nb_occurrences") > 1)

display(dim_client.join(doublons_df, "master_client_id"))

# Étape 4 : Tables de Faits Contrats et Sinistres (Gold Layer)

# Unification Contrats
facts_contrats = oracle_contrats.select(
    col("ID_CONTRAT").cast("string").alias("contrat_id"),
    lit("ORACLE").alias("source_system"),
    col("ID_CLIENT").cast("string").alias("original_client_id"),
    col("TYPE_CONTRAT").alias("type_contrat"),
    col("PRIME").alias("montant_prime")
).union(
    sqlserver_policies.select(
        col("PolicyID").alias("contrat_id"),
        lit("SQL_SERVER").alias("source_system"),
        col("CustomerID").alias("original_client_id"),
        col("PolicyType").alias("type_contrat"),
        col("Premium").alias("montant_prime")
    )
)

# Mappage avec Master_Client_ID
fact_contrat_gold = facts_contrats.join(
    dim_client,
    (facts_contrats.original_client_id == dim_client.original_id) & 
    (facts_contrats.source_system == dim_client.source_system)
).select("contrat_id", "master_client_id", "type_contrat", "montant_prime")

fact_contrat_gold.write.format("delta").mode("overwrite").saveAsTable("gold_fact_contrat")

# Unification Sinistres
fact_sinistre_gold = oracle_sinistres.select(
    col("ID_SINISTRE").cast("string").alias("sinistre_id"),
    col("ID_CONTRAT").cast("string").alias("contrat_id"),
    col("MONTANT").alias("montant_indemnise")
).union(
    sqlserver_claims.select(
        col("ClaimID").alias("sinistre_id"),
        col("PolicyID").alias("contrat_id"),
        col("Amount").alias("montant_indemnise")
    )
)

fact_sinistre_gold.write.format("delta").mode("overwrite").saveAsTable("gold_fact_sinistre")
"""
#Dossier 3 (Suite) : Tests unitaires, Qualité & Dashboards
#Q uestion 1 & 2 : Tests unitaires PySpark & Test Coverage
# Pour valider votre code (compatible avec les tests unitaires PySpark) :



import unittest

def clean_email(email_str):
    if not email_str:
        return None
    return email_str.strip().lower()

class TestDataQuality(unittest.TestCase):
    def test_clean_email(self):
        self.assertEqual(clean_email("  JEAN.DUPONT@Email.FR "), "jean.dupont@email.fr")
        self.assertIsNone(clean_email(None))

# Exécution des tests unitaires dans le Notebook
suite = unittest.TestLoader().loadTestsFromTestCase(TestDataQuality)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
Question 3 : Identification d'un bug potentiel dans le pipeline
•	Bug identifié : Clés primaires dupliquées après l'UNION simple.

•	Explication : Si AbAssurance et AssurePlus utilisent tous deux des identifiants numériques séquentiels (ID_CLIENT = 101 dans Oracle et CustomerID = 101 dans SQL Server), faire une simple jointure entraîne de fausses associations.

•	Correction : Toujours créer une clé composite (source_system + original_id) ou générer une clé surrogate unique via UUID / dense_rank() comme réalisé dans l'étape 3.

"""
# Création du Dashboard directement sous Databricks
# Pour réaliser les visualisations demandées : 


#1.	Créez une nouvelle cellule SQL dans votre notebook.

#2.	Exécutez les 4 requêtes ci-dessous.

#3.	Cliquez sur l'icône "+" à côté des résultats de la cellule -> Visualization, sélectionnez le type de graphique, puis cliquez sur Add to Dashboard. 
"""
#SQL
#-- 1. Répartition des clients par Ville (Graphique Camembert / Pie Chart)
SELECT ville, COUNT(DISTINCT master_client_id) as total_clients
FROM gold_dim_client
GROUP BY ville;

#-- 2. Statistiques Contrats (Graphique en Barres)
SELECT type_contrat, COUNT(contrat_id) as nb_contrats, SUM(montant_prime) as total_primes
FROM gold_fact_contrat
GROUP BY type_contrat;

#-- 3. Statistiques Sinistres & Financières (Indicateurs / Counter Widgets)
SELECT 
SUM(c.montant_prime) as Chiffre_Affaires_Total,
SUM(s.montant_indemnise) as Total_Indemnites_Versees,
ROUND((SUM(s.montant_indemnise) / SUM(c.montant_prime)) * 100, 2) as Ratio_Sinistralite_Pourcent
FROM gold_fact_contrat c
LEFT JOIN gold_fact_sinistre s ON c.contrat_id = s.contrat_id;

#-- 4. Matrice des Doublons (Tableau filtré)
SELECT master_client_id, nom, prenom, COUNT(*) as nombre_de_comptes
FROM gold_dim_client
GROUP BY master_client_id, nom, prenom
HAVING COUNT(*) > 1;


Résumé des livrables pour votre rendu :
1.	Document de cadrage/spécifications : Reprenez les sections du Dossier 1 (Sujet 1). 

2.	Schéma Data Flow : Exportez le schéma ASCII fourni dans le Dossier 3 (Sujet 1). 

3.	Agilité & Risques : Utilisez les tickets Trello et la matrice des risques rédigés dans le Sujet 2. 

4.	Code & Dashboard : Exécutez le script PySpark complet sous Databricks Community Edition pour générer les tables Delta et publier le tableau de bord interactif.

"""

