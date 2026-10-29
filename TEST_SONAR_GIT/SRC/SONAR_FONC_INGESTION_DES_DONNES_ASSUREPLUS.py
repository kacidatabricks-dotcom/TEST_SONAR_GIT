# Données AssurPlus SQLserver

from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col, lower, upper, trim, concat_ws, levenshtein, when, lit, count, sum as _sum, avg, round, to_date
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime
from pyspark.sql.window import Window

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DoubleType


# Initialisation Spark
spark = SparkSession.builder.appName("AssurePlusData").getOrCreate()

    
    # num fiscal et date creation n'existent pas 
    
    #rajouter score  le rajouter dans client final 









    #  A rajouter 
    #  StructField("AP_TRANSACTION_STATUS", StringType(), True)










def load_ap_data():
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

     ap_clients_data = [
        (5001,"","" ,"Jean Martin", "1985-03-15", "jean.martin@gmail.fr", "07 12 34 56 78", 
         "120 Avenue des Champs", "75008", "FR98765432101", "2021-01-10 09:00:00", "ACTIF",""),
        (5002,"" ,"" ,"Emilie Morin", "1991-11-25", "emilie.morin@assureplus.fr", "0723456789",  
         "45 Rue de la Paix", "69003", "FR87654321012", "2021-03-15 14:30:00", "ACTIF",""),
        (5003, "DUPONT","Marie","Marie DUPONT", "1990-07-22", "marie.dupont@gmail.fr", "0734567890",  
         "18 Boulevard Saint-Michel", "75005", "FR76543210923", "2021-06-20 11:15:00", "ACTIF",""),
        (5004, "VINCENT", "Laura","Laura Vincent", "1993-08-30", "laura.vincent@assureplus.fr", "0745678901", "52 Rue des Arts", "31000", "FR65432109834", "2021-09-25 16:45:00", "ACTIF",""),
        (5005, "GIRARD", "François","François Girard", "1984-12-17", "francois.girard@assureplus.fr", "0756789012", "8 Place de la Concorde", "44000", "FR54321098745", "2022-01-05 10:30:00", "ACTIF",""),
        (5006, "FOURNIER", "Cécile","Cécile Fournier", "1996-05-03", "cecile.fournier@assureplus.fr", "0767890123",  "33 Rue des Lilas Blancs", "59000", "FR43210987656", "2022-04-12 13:00:00", "ACTIF",""),
        (5007, "MOREAU", "Philippe","Philippe Moreau", "1976-10-19", "philippe.moreau@assureplus.fr", "0778901234", "27 Allée des Roses", "33000", "FR32109876567", "2022-07-18 08:45:00", "ACTIF",""),
        (5008, "LEROY", "Anne","anne Leroy", "1989-02-28", "anne.leroy@assureplus.fr", "0789012345",  
         "69 Rue du Faubourg", "76000", "FR21098765478", "2022-10-22 15:20:00", "ACTIF",""),
        (5009, "DAVID", "Julien","Julien David", "1994-07-14", "julien.david@assureplus.fr", "0790123456", "14 Boulevard de la Libération", "13003", "FR10987654389", "2023-01-30 12:10:00", "ACTIF",""),
         (5010, "MARTIN", "Valérie","Valérie Martin", "1981-03-22", "valerie.martin@assureplus.fr", "0701234567",  "86 Rue Nationale", "54000", "FR09876543290", "2023-03-14 09:40:00", "ACTIF","")
    ]

     ap_contrats_data = [
        ("ASP-2021-001", 5001, TYPE_AUTO, "2021-01-10", "2022-01-09", 920.00, "ACTIF", 201),
        ("ASP-2021-002", 5002, TYPE_HABITATION, "2022-01-05", "2023-01-04", 395.60, "ACTIF", 205),
        ("ASP-2022-006", 5006, TYPE_AUTO, "2022-04-12", "2023-04-11", 850.00, "ACTIF", 201),
        ("ASP-2022-007", 5007, TYPE_PREVOYANCE, "2022-07-18", "2023-07-17", 700.20, "ACTIF", 206),
        ("ASP-2022-008", 5008, TYPE_SANTE, "2022-10-22", "2023-10-21", 1450.40, "ACTIF", 203),
        ("ASP-2023-009", 5009, TYPE_AUTO, "2023-01-30", "2024-01-29", 945.75, "ACTIF", 202),
        ("ASP-2023-010", 5010, TYPE_AUTO, "2023-03-14", "2024-03-13", 430.90, "ACTIF", 205)
    ]

     ap_sinistres_data = [
        (8001, "ASP-2021-001", "2021-05-12 15:30:00", 1850.25, "REGLE", 
         "Accident sur autoroute, dégâts importants",0),
        (8002, "ASP-2021-002", "2021-07-28 10:15:00", 280.00, "REGLE",  
         "Fuite de canalisation, cuisine inondée",0),
        (8003, "ASP-2021-004", "2021-11-03 08:45:00", 3200.50, "EN_COURS",  
         "Carambolage avec 3 véhicules",0),
        (8004, "ASP-2022-005", "2022-02-18 19:20:00", 125.30, "REGLE",  
         "Bris de vitre, tentative d'effraction",0),
        (8005, "ASP-2022-006", "2022-06-30 14:10:00", 760.00, "EN_COURS",  
         "Dégâts suite à un orage, grêle",0),
        (8006, "ASP-2021-001", "2022-08-15 11:50:00", 980.40, "REGLE",  
         "Vol de roues sur le véhicule",0),
        (8007, "ASP-2022-008", "2022-12-20 16:30:00", 2800.00, "EN_COURS",  
         "Intervention chirurgicale urgente",0),
        (8008, "ASP-2023-009", "2023-03-05 07:40:00", 450.00, "REGLE",  
         "Petit accident, pare-chocs endommagé",0),
        (8009, "ASP-2022-007", "2023-05-18 09:00:00", 1500.00, "EN_COURS",  
         "Arrêt de travail prolongé, invalidité",0),
        (8010, "ASP-2023-010", "2023-07-25 13:25:00", 310.50, "NOUVEAU",  
         "Infiltration suite à fortes pluies",0)
    ]

     ap_payments_data = [
        (9001, "ASP-2021-001", "2021-01-15", 920.00, MODE_CARTE_BANCAIRE,"OK"),
        (9002, "ASP-2021-002", "2021-03-20", 450.50, MODE_PRELEVEMENT,"OK"),
        (9003, "ASP-2021-003", "2021-06-25", 660.40, MODE_VIREMENT,"OK"),
        (9004, "ASP-2021-004", "2021-09-30", 780.30, MODE_CARTE_BANCAIRE,"OK"),
        (9005, "ASP-2022-005", "2022-01-10", 395.60, MODE_PRELEVEMENT,"OK"),
        (9006, "ASP-2022-006", "2022-04-15", 850.00, MODE_CHEQUE,"OK"),
        (9007, "ASP-2022-007", "2022-07-20", 700.20, MODE_CARTE_BANCAIRE,"OK"),
        (9008, "ASP-2022-008", "2022-10-25", 725.20, MODE_VIREMENT,"OK"),
        (9009, "ASP-2023-009", "2023-02-05", 945.75, MODE_PRELEVEMENT,"OK"),
        (9010, "ASP-2023-010", "2023-03-18", 430.90, MODE_CARTE_BANCAIRE,"OK")
    ]
# création des DataFrames

     df_ap_clients = spark.createDataFrame(ap_clients_data, schema=ap_clients_schema)
     df_ap_contrats = spark.createDataFrame(ap_contrats_data, schema=ap_contrats_schema)
     df_ap_sinistres = spark.createDataFrame(ap_sinistres_data, schema=ap_sinistres_schema)
     df_ap_payments = spark.createDataFrame(ap_payments_data, schema=ap_payments_schema)

    
     # ⚡ ACTION FORCÉE : On oblige Spark à analyser les données maintenant
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

    

"""
    # Affichage
print("=== ASSUREPLUS - Clients ===")
df_ap_clients.show(10, truncate=False)

print("=== ASSUREPLUS - Contrats ===")
df_ap_contrats.show(10, truncate=False)

print("=== ASSUREPLUS - Sinistres ===")
df_ap_sinistres.show(10, truncate=False)

print("=== ASSUREPLUS - Payments ===")
df_ap_payments.show(10, truncate=False)
"""
