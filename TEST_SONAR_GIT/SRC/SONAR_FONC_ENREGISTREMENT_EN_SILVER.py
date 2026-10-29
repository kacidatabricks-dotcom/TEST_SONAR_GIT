#                                   ENREGISTREMENT EN SILVER        
# ATTENTION: Toutes les tables ne sont pas enregistrées dans la base de données delta


# ATTENTION: Toutes les tables ne sont pas enregistrées dans la base de données delta

#######  Créer une fonction pour la protection des contrats###################
'''
# Protection des données clients


def protection_silver(df2, source):
    

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


 


# Protection silver 
# pareil, le faire en notebook

df_ab_clients_silver = protection_clients_silver(df_ab_clients)
df_ap_clients_silver = protection_clients_silver(df_ap_clients)

df_ab_contrats_silver = protection_contrats_silver(df_ab_contrats)
df_ap_contrats_silver = protection_contrats_silver(df_ap_contrats)

'''



'''
def enregistrement_silver(df,tablename):

  df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tablename)
'''



def enregistrement_silver(df,TableName):
    try:
        print('écriture en Silver de la table...')
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableName)
        print("Table sauvegardée")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde en Silver de la table {TableName}: {e}")
    finally:
        print(f"Fin du traitement pour la table {TableName}")





# le faire dans le notebook
'''
# Tables Oracles
enregistrement_silver(df_ab_clients_silver,"silver.oracle_clients")
enregistrement_silver(df_ab_contrats_silver,"silver.oracle_contrats")
enregistrement_silver(df_ab_sinistres,"silver.oracle_sinistres")
enregistrement_silver(df_ab_payments,"silver.oracle_payments")

# Tables SQL Server
enregistrement_silver(df_ap_clients_silver,"silver.sqlserver_clients")
enregistrement_silver(df_ap_contrats_silver,"silver.sqlserver_contrats")
enregistrement_silver(df_ap_payments,"silver.sqlserver_payments")
enregistrement_silver(df_ap_sinistres,"silver.sqlserver_sinistres")
'''
