
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
- feature/us-01/importer-les-donnees-abassurance 
- feature/us-02/importer-les-donnees-assureplus  
- feature/us-03/normaliser-les-donnees-clients
- feature/us-04/identifier-les-doublons-clients
....  
 

```

Une Pull Request est utilisée pour intégrer une branche de fonctionnalité dans `develop`, puis `develop` vers `main`.
