AB_CLIENT_MAPPING = {
    "AB_CLIENT_ID": "source_client_id",
    "AB_NOM": "nom",
    "AB_PRENOM": "prenom",
    "AB_DATE_NAISSANCE": "date_naissance",
    "AB_EMAIL": "email",
    "AB_TELEPHONE": "telephone",
    "AB_ADRESSE": "adresse",
    "AB_CODE_POSTAL": "code_postal",
    "AB_NUM_FISCAL": "numero_fiscal",
    "AB_DATE_CREATION": "date_creation",
    "AB_STATUT_CLIENT": "statut_client",
}

AP_CLIENT_MAPPING = {
    "AP_USER_ID": "source_client_id",
    "AP_FULL_NAME": "nom_complet",
    "AP_BIRTH_DATE": "date_naissance",
    "AP_MAIL_ADDRESS": "email",
    "AP_PHONE_NUMBER": "telephone",
    "AP_STREET_ADDRESS": "adresse",
    "AP_ZIP_CODE": "code_postal",
    "AP_CREATED_AT": "date_creation",
    "AP_CUSTOMER_STATUS": "statut_client",
    "AP_LOYALTY_SCORE": "score_fidelite",
}

def rename_columns(df, mapping):
    for source, target in mapping.items():
        if source in df.columns:
            df = df.withColumnRenamed(source, target)
    return df
