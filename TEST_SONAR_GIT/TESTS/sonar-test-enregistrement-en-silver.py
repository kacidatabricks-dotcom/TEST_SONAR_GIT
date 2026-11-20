import importlib
enregistrement_silver = importlib.import_module("sonar-fonc-enregistrement-en-silver").enregistrement_silver


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
