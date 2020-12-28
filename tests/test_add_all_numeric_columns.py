from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.sql.functions import col
from operator import add
from functools import reduce
import atoolbox.spark.functions as tbs


def test_sum_all_numeric_cols(test_data):
    '''

    Returns:
        DataFrame: [description]
    '''
    df = tbs.add_numeric_fields(test_data["dfc"],"id")
    assert df.filter("id == 1").select(col("total")==4.01)