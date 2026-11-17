#

# Non 



    # ==============================================
    # ==============================================

"""
    # Protection des données
def protection_bronze(df2, source):
    
    salt = dbutils.secrets.get(scope="PROJET_BIG_DATA_STUDI", key="hash_salt")
    
    if source == "Oracle":
    

     df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AB_EMAIL"), F.lit(salt)), 256),

     "Nom_Hached": F.sha2(F.concat(F.col("AB_NOM"), F.lit(salt)), 256),
     "Prenom_Hached": F.sha2(F.concat(F.col("AB_PRENOM"), F.lit(salt)), 256),
     "Adresse_Hached": F.sha2(F.concat(F.col("AB_ADRESSE"), F.lit(salt)), 256),
     "Telephone_Hached": F.sha2(F.concat(F.col("AB_TELEPHONE"), F.lit(salt)), 256),
     "Num_fiscal_Hached": F.sha2(F.concat(F.col("AB_NUM_FISCAL"), F.lit(salt)), 256),
     "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AB_DATE_NAISSANCE"), F.lit(salt)), 256)}).drop("AB_NOM", "AB_PRENOM","AB_EMAIL","AB_DATE_NAISSANCE","AB_ADRESSE","AB_NUM_FISCAL","AB_TELEPHONE")
    else: 
    
     df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AP_MAIL_ADDRESS"), F.lit(salt)), 256),
     "FULL_NAME_Hached": F.sha2(F.concat(F.col("AP_FULL_NAME"), F.lit(salt)), 256),
     "Nom_Hached": F.sha2(F.concat(F.col("AP_NOM"), F.lit(salt)), 256),
     "Prenom_Hached": F.sha2(F.concat(F.col("AP_PRENOM"), F.lit(salt)), 256),
     "Telephone_Hached": F.sha2(F.concat(F.col("AP_PHONE_NUMBER"), F.lit(salt)), 256),
     "Street_Hached": F.sha2(F.concat(F.col("AP_STREET_ADDRESS"), F.lit(salt)), 256),
     "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AP_BIRTH_DATE"), F.lit(salt)), 256)}).drop("AP_FULL_NAME","AP_MAIL_ADDRESS","AP_BIRTH_DATE","AP_NOM", "AP_PRENOM","AP_PHONE_NUMBER","AP_STREET_ADDRESS")
    return df2

"""


