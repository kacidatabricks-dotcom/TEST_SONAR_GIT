# Sécurité des données par rôle — Dashboard Assurance

Mise en œuvre : Unity Catalog (masquage dynamique de colonnes + filtres de lignes), le 23/11/2026.

## 1. Objectif

L'objectif est de restreindre l'accès aux données sensibles du projet d'assurance selon le rôle de chaque utilisateur, au niveau des tables Gold (et non au niveau du dashboard, ce que AI/BI Dashboards ne permet pas) :

- masquer les données personnelles des clients (PII) pour les rôles non autorisés ;
- réserver les données de sinistres aux rôles concernés ;
- laisser l'accès complet aux données pour le pilotage technique et la direction générale.

## 2. Matrice d'accès

| Rôle | Données personnelles clients (PII) | Sinistres | Contrats / Paiements |
| --- | --- | --- | --- |
| Data Engineer | Complètes | Complètes | Complets |
| Directeur général | Complètes | Complètes | Complets |
| Responsable des sinistres | Masquées (*** ) | Complètes | Complets |
| Data Analyst | Masquées (*** ) | Aucune ligne | Complets |
| Directeur financier | Masquées (*** ) | Aucune ligne | Complets |
| Directeur Commercial | Masquées (*** ) | Aucune ligne | Complets |

Les PII concernées : email, nom, prénom, date de naissance, numéro fiscal, adresse, clé de rapprochement, téléphone (8 colonnes de `workspace.gold.dim_clients_gold`).

## 3. Mise en œuvre technique

### 3.1 Schéma de sécurité

Un schéma dédié `workspace.security` contient les deux fonctions de sécurité :

- `mask_pii(v STRING)` : retourne la valeur en clair si l'utilisateur appartient au groupe *Data Engineer* ou *Directeur général* ; sinon retourne `***`.
- `sinistres_filter(statut STRING)` : retourne vrai si l'utilisateur appartient aux groupes *Data Engineer*, *Directeur général* ou *Responsable des sinistres* ; sinon les lignes sont exclues.

Les deux fonctions s'appuient sur `is_account_group_member('<groupe>')`, qui identifie le rôle de l'utilisateur lançant la requête.

### 3.2 Application aux tables

- `workspace.gold.dim_clients_gold` : masque de colonne appliqué sur les 8 colonnes PII (via `ALTER COLUMN ... SET MASK workspace.security.mask_pii`).
- `workspace.gold.fact_sinistres` : filtre de lignes appliqué (via `SET ROW FILTER workspace.security.sinistres_filter`).
- `workspace.gold.fact_contrats` et `fact_payments` : aucune restriction (accès complet à tous les rôles autorisés).

Le code postal (`CODE_POSTAL`) reste visible par tous car il sert de filtre global au dashboard.

### 3.3 Droits accordés

Les six groupes (Data Engineer, Data Analyst, Directeur financier, Responsable des sinistres, Directeur général, Directeur Commercial) disposent de :

- `USE CATALOG` sur le catalog `workspace` ;
- `USE SCHEMA` + `SELECT` sur le schéma `workspace.gold` ;
- `USE SCHEMA` sur `workspace.security` et `EXECUTE` sur les deux fonctions.

Les fonctions étant évaluées pour l'utilisateur qui interroge la table, chaque rôle ne voit que ses données autorisées, quel que soit le dashboard utilisé.

## 4. Dashboards

- `Dashboard_Projet_Big_Data_STUDI` : dashboard de référence, inchangé.
- `Dashboard_Assurance_Securise` : réplique identique destinée aux rôles restreints ; la sécurité étant appliquée au niveau des tables, les restrictions s'y appliquent automatiquement selon le rôle connecté.

## 5. Procédure de test (soutenance)

1. Retirer temporairement le compte de test des groupes « Data Engineer », « Directeur général » et « Responsable des sinistres » (Admin Settings → Groups).
2. Ouvrir `Dashboard_Assurance_Securise` : les données personnelles s'affichent `***` et la page « Sinistres Détaillés » est vide.
3. Ajouter le compte au groupe « Responsable des sinistres » : les sinistres réapparaissent, les PII restent masquées.
4. Ajouter le compte au groupe « Data Engineer » : toutes les données s'affichent en clair.