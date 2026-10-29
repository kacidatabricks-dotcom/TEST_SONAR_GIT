"""
# Données AssurPlus SQLserver


# Initialisation Spark
# spark = SparkSession.builder.appName("AssurePlusData").getOrCreate()





ap_payments_schema = StructType([
 
    #  A rajouter 
      StructField("AP_TRANSACTION_STATUS", StringType(), True)





"""
def test_load_ap_data():
    df_clients, df_contrats, df_sinistres, df_payments = load_ap_data()
    assert df_clients.count() == 10
    assert df_contrats.count() == 10
    assert df_sinistres.count() == 10
    assert df_payments.count() == 10
    display(df_clients)
    display(df_contrats)
    display(df_sinistres)
    display(df_payments)