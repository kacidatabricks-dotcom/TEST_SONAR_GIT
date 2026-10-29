# Il faut enregsitrer les versions hachées


#                                   ENREGISTREMENT EN BRONZE        
# ATTENTION: Toutes les tables ne sont pas enregistrées dans la base de données delta

#######  Créer une fonction pour la protection des contrats###################
'''
# Protection des données clients
 
# Protection des données
def protection_bronze(df2, source):
    

    salt = dbutils.secrets.get(scope="PROJET_BIG_DATA_STUDI", key="hash_salt")
    
    if source == "Oracle":
    

     df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AB_EMAIL"), F.lit(salt)), 256),

     "Nom_Hached": F.sha2(F.concat(F.col("AB_NOM"), F.lit(salt)), 256),
     "Prenom_Hached": F.sha2(F.concat(F.col("AB_PRENOM"), F.lit(salt)), 256),
     "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AB_DATE_NAISSANCE"), F.lit(salt)), 256)}).drop("AB_NOM", "AB_PRENOM","AB_EMAIL","AB_DATE_NAISSANCE")
    else: 
    
     df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AP_MAIL_ADDRESS"), F.lit(salt)), 256),
     "FULL_NAME_Hached": F.sha2(F.concat(F.col("AP_FULL_NAME"), F.lit(salt)), 256),
     "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AP_BIRTH_DATE"), F.lit(salt)), 256)}).drop("AP_FULL_NAME","AP_MAIL_ADDRESS","AP_BIRTH_DATE","AP_NOM", "AP_PRENOM")
    return df2


   


# Protection bronze
#il faut faire dans le notebook

df_ab_clients_bronze = protection_clients_bronze(df_ab_clients)
df_ap_clients_bronze = protection_clients_bronze(df_ap_clients)

df_ab_contrats_bronze = protection_contrats_bronze(df_ab_contrats)
df_ap_contrats_bronze = protection_contrats_bronze(df_ap_contrats)

'''
'''
def enregistrement_bronze(df,tablename):

 df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tablename)
'''
def enregistrement_bronze(df,TableName):
    try:
        print('écriture en Bronze de la table...')
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableName)
        print("Table sauvegardée")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde en Bronze de la table {TableName}: {e}")
    finally:
        print(f"Fin du traitement pour la table {TableName}")



"""
# Tables Oracles
enregistrement_bronze(df_ab_clients_bronze,"bronze.oracle_clients")
enregistrement_bronze(df_ab_contrats_bronze,"bronze.oracle_contrats")
enregistrement_bronze(df_ab_sinistres_bronze,"bronze.oracle_sinistres")
enregistrement_bronze(df_ab_payments_bronze,"bronze.oracle_payments")

# Tables SQL Server
enregistrement_bronze(df_ap_clients_bronze,"bronze.sqlserver_clients")
enregistrement_bronze(df_ap_contrats_bronze,"bronze.sqlserver_contrats")
enregistrement_bronze(df_ap_payments_bronze,"bronze.sqlserver_payments")
enregistrement_bronze(df_ap_sinistres_bronze,"bronze.sqlserver_sinistres")

"""
    