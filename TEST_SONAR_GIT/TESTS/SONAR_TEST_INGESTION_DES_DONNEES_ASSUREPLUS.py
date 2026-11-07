"""
# Données AssurPlus SQLserver


# Initialisation Spark
# spark = SparkSession.builder.appName("AssurePlusData").getOrCreate()





ap_payments_schema = StructType([
 
    #  A rajouter 
      StructField("AP_TRANSACTION_STATUS", StringType(), True)





"""
from SONAR_FONC_INGESTION_DES_DONNEES_ASSUREPLUS import load_ap_data
def test_load_ap_data(spark):
    df_clients, df_contrats, df_sinistres, df_payments = load_ap_data()
    assert df_clients.count() == 10
    assert df_contrats.count() == 7
    assert df_sinistres.count() == 10
    assert df_payments.count() == 10
    df_clients.show(truncate=False)
    df_contrats.show(truncate=False)
    df_sinistres.show(truncate=False)
    df_payments.show(truncate=False)