# Stratégie Git

## Branches permanentes
- `main` : version stable/livrable
- `develop` : intégration des fonctionnalités

## Branches temporaires
- `feature/<fonctionnalite>`
- `fix/<bug>`
- `test/<fonctionnalite>`
- `refactor/<fonctionnalite>`
- `docs/<sujet>`

## Exemple
```text
main
  |
develop
- Feature/US-01 — Importer les données AbAssurance 
- Feature/US-02 — Importer les données AssurePlus 
- Feature/US-03 — Normaliser les données clients 
- Feature/US-04 — Identifier les doublons 
- Feature/US-05 — Construire le modèle final 
- Feature /US-06 — Répartition clients 
- Feature/US-07 — Statistiques contrats 
- Feature/US-08 — Statistiques sinistres 
- Feature/US-09 — Statistiques financières 
- Feature/US-10 — Matrice de doublons 
- Feature/US-11 — Gestion des accès (RBAC) 
- Feature/US-12 — Hachage des données sensibles 

```

Une Pull Request est utilisée pour intégrer une branche de fonctionnalité dans `develop`, puis `develop` vers `main`.
