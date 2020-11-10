from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.sql.functions import col
from operator import add
from functools import reduce
import atoolbox.spark.functions as tbs


sa = StructType([
    StructField('id', IntegerType(), False),
    StructField('tag', StringType(), False),
    StructField('a1', IntegerType(), False),
    StructField('a2', IntegerType(), False),
    ])
    

sb = StructType([
    StructField('id', IntegerType(), False),
    StructField('tag', StringType(), False),
    StructField('b1', LongType(), False),
    StructField('b2', IntegerType(), False),
    ])

sc = StructType([
    StructField('id', IntegerType(), False),
    StructField('tag', StringType(), False),
    StructField('c1', LongType(), False),
    StructField('c2', FloatType(), False),
    StructField('c3', IntegerType(), False),
    ])

da = [
        (1,'a',1,1),
        (2,'a',1,1),
        (3,'a',1,1)]

db = [
        (1,'b',2,2),
        (2,'b',2,2),
        (3,'b',2,2)]

dc = [
        (1,'c',3,0.010,1),
        (2,'c',3,3.0,0),
        (3,'c',3,3.0,1)]

spark = (SparkSession.builder
    .config("master", "local[*]")
    .appName("test-session")
    .getOrCreate())

dfa = spark.createDataFrame(da, sa)
dfb = spark.createDataFrame(db, sb)
dfc = spark.createDataFrame(dc, sc)


def test_sum_all_numeric_cols():
    '''

    Returns:
        DataFrame: [description]
    '''
    df = tbs.add_numeric_fields(dfc,"id")
    assert df.filter("id == 1").select(col("total")==4.01)