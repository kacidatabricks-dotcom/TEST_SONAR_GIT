# Stratégie de tests

## Tests unitaires
Testent les fonctions Python :
- normalisation email
- normalisation téléphone
- parsing des dates
- score de matching
- classification des doublons
- mapping

## Tests d'intégration
Vérifient :
- lecture des données
- écriture Bronze
- transformation Silver
- production Gold

## Tests de données
Vérifient :
- unicité
- nullité
- formats
- contraintes métier

## Non-régression
Un jeu de données de référence est conservé afin de vérifier que les KPI ne changent pas sans raison.

## Coverage
Objectif pédagogique : >= 80 %, idéalement >= 90 %.
