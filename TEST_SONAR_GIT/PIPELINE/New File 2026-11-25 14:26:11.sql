SELECT is_account_group_member('Responsable des sinistres') AS resp,
       COUNT(*) AS nb_sinistres
FROM workspace.gold.fact_sinistres
