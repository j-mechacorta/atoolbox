from pyspark.sql import SparkSession
import pytest
import atoolbox.spark.functions as tbs 


schemaa = 'id: INT, a: STRING'
schemab = 'id: INT, b: STRING'
schemac = 'id: INT, b: STRING'
    
a = [
        (1,'a'),
        (2,'a'),
        (3,'a')]

b = [

        (2,'b'),
        (3,'b'),
        (4,'b')]

c = [
        (1,'c'),
        (2,'c'),
        (3,'c'),
        (4,'c')]

spark = (SparkSession.builder
    .config("master", "local[*]")
    .appName("test-session")
    .getOrCreate())

dfa = spark.createDataFrame(a, schemaa)
dfb = spark.createDataFrame(b, schemab)
dfc = spark.createDataFrame(c, schemac)

def test_left_join_dataframes():
    df_list = [dfa, dfb]
    expected_cols = ['id', 'a', 'b']
    joined_df = tbs.join_dataframes(df_list, on="id", how="left")
    print(expected_cols)
    print(joined_df.columns)
    assert joined_df.columns == expected_cols

def test_inner_join_dataframes():
    pass