# Architecture

## Choix pour la Free Edition


Le sujet demande Kafka, Talend, Hadoop et Spark. Pour une implémentation pédagogique 100 % Databricks :
- PySpark couvre l'ETL et le traitement distribué ;
- Delta Lake couvre le stockage analytique transactionnel ;
- les jobs/workflows couvrent l'orchestration ;
- le streaming Databricks peut couvrir le besoin temps réel lorsque l'environnement le permet ;
- le dashboard Databricks couvre la restitution.

La Free Edition est utilisée pour démontrer le pipeline. Les sources Oracle et SQL Server sont simulées par des exports CSV afin de ne pas dépendre d'une infrastructure réseau externe.

## Production

```text
Oracle ----JDBC/CDC----\
                         > Ingestion -> Bronze -> Silver -> Gold -> SQL/Dashboard
SQL Server --JDBC/CDC--/
```

## Prototype

```text
AB CSV --------\
                > Spark -> Bronze -> Silver -> Gold -> Dashboard
AP CSV --------/
```

## Pourquoi Bronze/Silver/Gold ?

Bronze :
- conservation proche de la source
- traçabilité

Silver :
- nettoyage
- normalisation
- mapping
- contrôles

Gold :
- modèle métier
- KPI
- restitution
