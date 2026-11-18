# Données AssurPlus SQLserver

from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col, lower, upper, trim, concat_ws, levenshtein, when, lit, count, sum as _sum, avg, round, to_date
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DoubleType
from pathlib import Path

<<<<<<< HEAD
def load_ap_data(spark):
=======
# Initialisation Spark
# spark = SparkSession.builder.appName("AssurePlusData").getOrCreate()  supprimé parès conseil de chat

    
    # num fiscal et date creation n'existent pas 
    
    #rajouter score  le rajouter dans client final 









    #  A rajouter 
    #  StructField("AP_TRANSACTION_STATUS", StringType(), True)










def load_ap_data(spark,data_dir=None):
    if data_dir is None:
        data_dir = (
            Path(__file__).resolve().parents[1]
            / "DATA"
            / "SAMPLE"
            / "APASSURANCE"
        ).as_uri()

    data_dir = data_dir.rstrip("/")
>>>>>>> origin/Develop
    df_ap_clients= None
    df_ap_contrats = None
    df_ap_sinistres = None
    df_ap_payments = None
    try:
     print("Création des dataframes AssurePlus en cours...")
     # Constantes de valeurs pour types et modes
     TYPE_SANTE = "Santé"
     TYPE_HABITATION = "Habitation"
     TYPE_AUTO = "Auto"
     TYPE_PREVOYANCE = "Prévoyance"

     MODE_CARTE_BANCAIRE = "Carte bancaire"
     MODE_VIREMENT = "Virement"
     MODE_PRELEVEMENT = "Prélèvement"
     MODE_CHEQUE = "Chèque"
     # Initialisation des schémas et données AssurPlus
     ap_clients_schema = StructType([
        StructField("AP_USER_ID", IntegerType(), True),
        StructField("AP_NOM", StringType(), True),
        StructField("AP_PRENOM", StringType(), True),
        StructField("AP_FULL_NAME", StringType(), True),
        StructField("AP_BIRTH_DATE", StringType(), True),
        StructField("AP_MAIL_ADDRESS", StringType(), True),
        StructField("AP_PHONE_NUMBER", StringType(), True),
        StructField("AP_STREET_ADDRESS", StringType(), True),
        StructField("AP_ZIP_CODE", StringType(), True),
        StructField("AP_NUM_FISCAL", StringType(), True),
        StructField("AP_DATE_CREATION", StringType(), True),
        StructField("AP_CUSTOMER_STATUS", StringType(), True),
        StructField("AP_LOYALTY_SCORE", StringType(), True)
    ])

     ap_contrats_schema = StructType([
        StructField("AP_CONTRACT_REF", StringType(), True),
        StructField("AP_USER_ID", IntegerType(), True),
        StructField("AP_PRODUCT_CODE", StringType(), True),
        StructField("AP_START_DATE", StringType(), True),
        StructField("AP_END_DATE", StringType(), True),
        StructField("AP_MONTHLY_PREMIUM", DoubleType(), True),
        StructField("AP_CONTRACT_STATE", StringType(), True),
        StructField("AP_BROKER_CODE", IntegerType(), True)
    ])

     ap_sinistres_schema = StructType([
        StructField("AP_SINISTRE_NUM", IntegerType(), True),
        StructField("AP_CONTRACT_REF", StringType(), True),
        StructField("AP_INCIDENT_DATE", StringType(), True),
        StructField("AP_ESTIMATED_AMOUNT", DoubleType(), True),
        StructField("AP_CLAIM_STATUS", StringType(), True),
        StructField("AP_CLAIM_COMMENT", StringType(), True),
        StructField("AP_FRAUDE_SCORE", StringType(), True),
    ])

     ap_payments_schema = StructType([
        StructField("AP_PAYMENT_REF", IntegerType(), True),
        StructField("AP_CONTRACT_REF", StringType(), True),
        StructField("AP_PAYMENT_DATETIME", StringType(), True),
        StructField("AP_AMOUNT_PAID", DoubleType(), True),
        StructField("AP_PAYMENT_CHANNEL", StringType(), True),
        StructField("AP_TRANSACTION_STATUS", StringType(), True)
    ])

       
# création des DataFrames

     df_ap_clients = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", "true") 
<<<<<<< HEAD
         .load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/APASSURANCE/Clients.csv"))
=======
         #.load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/APASSURANCE/Clients.csv")) # NOSONAR
         .load(f"{data_dir}/Clients.csv"))
>>>>>>> origin/Develop

     df_ap_contrats = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", "true") 
<<<<<<< HEAD
        .load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/APASSURANCE/Contrats.csv"))
=======
        #.load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/APASSURANCE/Contrats.csv")) # NOSONAR
        .load(f"{data_dir}/Contrats.csv"))
>>>>>>> origin/Develop
     # Rendre le montant de la prime annuelle comme dans AB Assurance
     df_ap_contrats = df_ap_contrats.withColumn("AP_MONTHLY_PREMIUM",round(col("AP_MONTHLY_PREMIUM").cast("double")*12,2))


     df_ap_sinistres = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", "true") 
<<<<<<< HEAD
        .load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/APASSURANCE/Sinistres.csv"))
=======
        #.load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/APASSURANCE/Sinistres.csv")) # NOSONAR
        .load(f"{data_dir}/Sinistres.csv"))
>>>>>>> origin/Develop

     df_ap_payments = (
        spark.read.format("csv")
        .option("header", True)
<<<<<<< HEAD
        .option("inferSchema", "true") 
        .load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/APASSURANCE/Payments.csv"))
     
=======
        .option("inferSchema", "true")
        #.load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/APASSURANCE/Payments.csv")) # NOSONAR
       .load(f"{data_dir}/Payments.csv"))
     '''
     df_ap_clients = spark.createDataFrame(ap_clients_data, schema=ap_clients_schema)
     df_ap_contrats = spark.createDataFrame(ap_contrats_data, schema=ap_contrats_schema)
     df_ap_sinistres = spark.createDataFrame(ap_sinistres_data, schema=ap_sinistres_schema)
     df_ap_payments = spark.createDataFrame(ap_payments_data, schema=ap_payments_schema)
     '''
>>>>>>> origin/Develop
    

     df_ap_clients.first() 
     df_ap_contrats.first() 
     df_ap_sinistres.first() 
     df_ap_payments.first() 
        
     print("Création dataframes réussie.")
     return df_ap_clients, df_ap_contrats, df_ap_sinistres, df_ap_payments   
    except Exception as e:
        print("\n--- ÉCHEC DU PIPELINE ---")
        print("La création des dataframes AssurePlus a échoué.")
        print(f"Détail technique de l'erreur : {e}")
        # On lève à nouveau l'erreur pour empêcher la suite du script de tourner avec dtaframes vides
        raise e

    


