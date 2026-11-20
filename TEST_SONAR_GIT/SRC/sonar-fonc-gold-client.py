# Enregistrment GOLD Client
# fait dans le programme principal

def save_dim_clients_to_gold(df,TableName):
    try:
        print('écriture en Gold de la table...')
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableName)
        print("Table sauvegardée")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde en Gold de la table {TableName}: {e}")
    finally:
        print(f"Fin du traitement pour la table {TableName}")


