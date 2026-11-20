#                                 Enregistrement en Bronze
def enregistrement_bronze(df,TableName):
    try:
        print('écriture en Bronze de la table...')
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableName)
        print("Table sauvegardée")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde en Bronze de la table {TableName}: {e}")
    finally:
        print(f"Fin du traitement pour la table {TableName}")



    