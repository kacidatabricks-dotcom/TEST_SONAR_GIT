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
  +-- feature/ingestion
  +-- feature/data-cleaning
  +-- feature/deduplication
  +-- feature/dashboard
  +-- test/data-quality
  +-- fix/idempotence
```

Une Pull Request est utilisée pour intégrer une branche de fonctionnalité dans `develop`, puis `develop` vers `main`.
