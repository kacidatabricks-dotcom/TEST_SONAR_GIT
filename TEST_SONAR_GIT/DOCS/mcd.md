# MCD cible proposé

Entités :
- CLIENT
- CONTRAT
- SINISTRE
- PAIEMENT

Relations :
- CLIENT 1,N CONTRAT
- CONTRAT 1,N SINISTRE
- CONTRAT 1,N PAIEMENT

## Identifiants techniques

`client_id`, `contract_id`, `claim_id`, `payment_id`

## Traçabilité

Chaque entité conserve :
- `source_system`
- `source_*_id`

Cela permet de relier une ligne consolidée à son système d'origine.

## CLIENT
client_id
source_system
source_client_id
nom
prenom
nom_complet
date_naissance
email
telephone
adresse
code_postal
numero_fiscal
date_creation
statut_client
score_fidelite
date_integration

## CONTRAT
contract_id
contract_reference
client_id
source_system
source_contract_id
type_assurance
date_debut
date_fin
prime_annuelle
prime_mensuelle
statut_contrat
agence_code
broker_code

## SINISTRE
claim_id
contract_id
source_system
source_claim_id
date_sinistre
montant_estime
statut_sinistre
description
fraud_score

## PAIEMENT
payment_id
contract_id
source_system
source_payment_id
date_paiement
montant
mode_paiement
statut_transaction
