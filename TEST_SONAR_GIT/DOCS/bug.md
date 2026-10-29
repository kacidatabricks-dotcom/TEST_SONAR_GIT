# Bug potentiel identifié

## Bug
Le pipeline utilise `append` à chaque exécution.

Si un job est relancé après un échec partiel :
- les mêmes lignes peuvent être réinsérées ;
- les KPI deviennent faux ;
- les clients/contrats peuvent être comptés deux fois.

## Correction
Construire une clé métier stable et utiliser une logique de type MERGE/upsert.

Exemple de clé client :
`source_system + source_client_id`

Pour un contrat :
`source_system + source_contract_id`

Le pipeline devient idempotent :
exécuter deux fois le même lot produit le même résultat que l'exécuter une seule fois.

