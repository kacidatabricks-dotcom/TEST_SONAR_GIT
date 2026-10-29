# Data mapping

## CLIENT

| Source | Colonne source | Cible | Transformation |
|---|---|---|---|
| AB | AB_CLIENT_ID | source_client_id | cast entier/string |
| AB | AB_NOM | nom | trim + initcap |
| AB | AB_PRENOM | prenom | trim + initcap |
| AB | AB_DATE_NAISSANCE | date_naissance | to_date |
| AB | AB_EMAIL | email | trim + lower |
| AB | AB_TELEPHONE | telephone | normalisation |
| AB | AB_ADRESSE | adresse | trim |
| AB | AB_CODE_POSTAL | code_postal | trim |
| AB | AB_NUM_FISCAL | numero_fiscal | trim |
| AB | AB_DATE_CREATION | date_creation | timestamp |
| AB | AB_STATUT_CLIENT | statut_client | trim/lower |

| AP | Colonne source | Cible | Transformation |
|---|---|---|---|
| AP | AP_USER_ID | source_client_id | cast entier/string |
| AP | AP_FULL_NAME | nom/nom_complet | normalisation ; split à documenter |
| AP | AP_BIRTH_DATE | date_naissance | to_date yyyy-MM-dd |
| AP | AP_MAIL_ADDRESS | email | trim + lower |
| AP | AP_PHONE_NUMBER | telephone | normalisation |
| AP | AP_STREET_ADDRESS | adresse | trim |
| AP | AP_ZIP_CODE | code_postal | trim |
| AP | AP_CREATED_AT | date_creation | timestamp |
| AP | AP_CUSTOMER_STATUS | statut_client | trim/lower |
| AP | AP_LOYALTY_SCORE | score_fidelite | cast int |

## CONTRAT

AB :
- AB_POLICY_NUMBER -> contract_reference
- AB_CLIENT_ID -> source_client_id_client
- AB_TYPE_ASSURANCE -> type_assurance
- AB_DATE_DEBUT -> date_debut
- AB_DATE_FIN -> date_fin
- AB_PRIME_ANNUELLE -> prime_annuelle
- AB_STATUT_CONTRAT -> statut_contrat
- AB_AGENCE_ID -> agence_code

AP :
- AP_CONTRACT_REF -> contract_reference
- AP_USER_ID -> source_client_id_client
- AP_PRODUCT_CODE -> type_assurance
- AP_START_DATE -> date_debut
- AP_END_DATE -> date_fin
- AP_MONTHLY_PREMIUM -> prime_mensuelle
- AP_CONTRACT_STATE -> statut_contrat
- AP_BROKER_CODE -> broker_code

## SINISTRE

AB :
- AB_CLAIM_ID -> source_claim_id
- AB_POLICY_NUMBER -> contract_reference
- AB_DATE_SINISTRE -> date_sinistre
- AB_MONTANT_ESTIME -> montant_estime
- AB_STATUT_SINISTRE -> statut_sinistre
- AB_DESCRIPTION -> description

AP :
- AP_SINISTRE_NUM -> source_claim_id
- AP_CONTRACT_REF -> contract_reference
- AP_INCIDENT_DATE -> date_sinistre
- AP_ESTIMATED_AMOUNT -> montant_estime
- AP_CLAIM_STATUS -> statut_sinistre
- AP_CLAIM_COMMENT -> description
- AP_FRAUD_SCORE -> fraud_score

## PAIEMENT

AB :
- AB_PAYMENT_ID -> source_payment_id
- AB_POLICY_NUMBER -> contract_reference
- AB_DATE_PAIEMENT -> date_paiement
- AB_MONTANT -> montant
- AB_MODE_PAIEMENT -> mode_paiement

AP :
- AP_PAYMENT_REF -> source_payment_id
- AP_CONTRACT_REF -> contract_reference
- AP_PAYMENT_DATETIME -> date_paiement
- AP_AMOUNT_PAID -> montant
- AP_PAYMENT_CHANNEL -> mode_paiement
- AP_TRANSACTION_STATUS -> statut_transaction
