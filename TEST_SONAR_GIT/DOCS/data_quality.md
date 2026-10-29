# Data Quality

## Règles proposées

### Client
- identifiant source non nul
- email normalisé
- email syntaxiquement valide
- téléphone normalisé
- date de naissance valide
- statut dans une liste de valeurs autorisées
- absence de doublon sur la clé source

### Contrat
- référence non nulle
- date de début <= date de fin
- prime >= 0
- client de rattachement existant

### Sinistre
- référence non nulle
- montant >= 0
- date valide
- contrat existant

### Paiement
- référence non nulle
- montant >= 0
- date valide
- contrat existant

## Stratégie

Les lignes invalides peuvent être :
1. rejetées dans une table/quarantine ;
2. corrigées si une règle déterministe existe ;
3. conservées avec un indicateur de qualité si elles sont exploitables.

Ne jamais supprimer silencieusement une donnée source.


## Matching client
Score proposé : email 50, téléphone 30, nom 10, date de naissance 20, soit 110 maximum.
