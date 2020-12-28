from pyspark.sql import SparkSession
import pytest
import atoolbox.spark.functions as tbs 


def test_left_join_dataframes(test_data):
    df_list = [test_data["dfa"].select("id", "a1"), test_data["dfb"].select("id", "b1"), test_data["dfc"].select("id", "c1")]
    expected_cols = ['id', 'a1', 'b1', 'c1']
    joined_df = tbs.join_dataframes(df_list, on="id", how="left")
    print(expected_cols)
    print(joined_df.columns)
    assert joined_df.columns == expected_cols

def test_inner_join_dataframes():
    pass