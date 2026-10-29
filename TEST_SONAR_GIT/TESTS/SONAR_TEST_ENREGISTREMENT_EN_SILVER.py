"""
#                                   ENREGISTREMENT EN SILVER        
# ATTENTION: Toutes les tables ne sont pas enregistrées dans la base de données delta
def enregistrement_silver(df,tablename):

 df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tablename)

# Tables Oracles
enregistrement_silver(df_ab_clients,"silver.oracle_clients")
enregistrement_silver(df_ab_contrats,"silver.oracle_contrats")
enregistrement_silver(df_ab_sinistres,"silver.oracle_sinistres")
enregistrement_silver(df_ab_payments,"silver.oracle_payments")

# Tables SQL Server
enregistrement_silver(df_ap_clients,"silver.sqlserver_clients")
enregistrement_silver(df_ap_contrats,"silver.sqlserver_contrats")
enregistrement_silver(df_ap_payments,"silver.sqlserver_payments")
enregistrement_silver(df_ap_sinistres,"silver.sqlserver_sinistres")

"""
def test_enregistrement_silver():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    sample_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    sample_data = [(1, "Alice"), (2, "Bob")]
    df_test = spark.createDataFrame(sample_data, schema=sample_schema)
    test_table_name = "silver.test_table"
    
    enregistrement_silver(df_test, test_table_name)

    df_result = spark.table(test_table_name)
    assert df_result.count() == 2
    assert set(df_result.columns) == {"id", "name"}
