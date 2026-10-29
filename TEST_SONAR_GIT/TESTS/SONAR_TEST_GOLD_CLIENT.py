"""
# Enregistrment GOLD Client
dim_client.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_client")

def save_dim_client_to_gold(dim_client):
    dim_client.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_client")
"""
def test_save_dim_client_to_gold():
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    test_data = [(1, "Alice"), (2, "Bob")]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    test_df = spark.createDataFrame(test_data, schema)
    save_dim_clients_to_gold(test_df,"gold.test_dim_clients")
    result_df = spark.table("gold.test_dim_clients")
    assert result_df.count() == 2
    assert set(result_df.columns) == {"id", "name"}

test_save_dim_client_to_gold()