-- KPI 1 : clients
SELECT source_system, COUNT(*) AS nb_clients
FROM gold_client
GROUP BY source_system;

-- KPI 2 : contrats
SELECT source_system, statut_contrat, COUNT(*) AS nb_contrats
FROM gold_contrat
GROUP BY source_system, statut_contrat;

-- KPI 3 : sinistres
SELECT source_system, statut_sinistre,
       COUNT(*) AS nb_sinistres,
       SUM(montant_estime) AS montant_total
FROM gold_sinistre
GROUP BY source_system, statut_sinistre;

-- KPI 4 : paiements
SELECT source_system,
       COUNT(*) AS nb_paiements,
       SUM(montant) AS total_paiements
FROM gold_paiement
GROUP BY source_system;

-- KPI 5 : doublons
SELECT decision, COUNT(*) AS nb
FROM gold_duplicate_matrix
GROUP BY decision;
