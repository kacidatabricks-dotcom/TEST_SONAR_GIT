#                                   ENREGISTREMENT EN SILVER        




def enregistrement_silver(df,TableName):
    try:
        print('écriture en Silver de la table...')
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableName)
        print("Table sauvegardée")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde en Silver de la table {TableName}: {e}")
    finally:
        print(f"Fin du traitement pour la table {TableName}")



