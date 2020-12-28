import time
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pytest
from colorama import Fore, Back, Style
from hooks import *





@pytest.fixture(scope='session', autouse=True)
def spark_session():
    logger = get_logger('create_spark_session')
    logger.info("creating spark session")
    start = time.time()
    # start_testing()
    res = SparkSession.builder.getOrCreate()
    logger.info(f"spark session init time: " + Fore.RED + f"{time.time() - start}")
    return res

@pytest.fixture(scope='session', autouse=True)
def test_data(spark_session):

    sa = (
        T.StructType()
        .add('id', T.IntegerType(), False, None)
        .add('tag', T.StringType(), False, None)
        .add('a1', T.IntegerType(), False, None)
        .add('a2', T.IntegerType(), False, None)
        )
        

    sb = (
        T.StructType()
        .add('id', T.IntegerType(), False, None)
        .add('tag', T.StringType(), False, None)
        .add('b1', T.LongType(), False, None)
        .add('b2', T.IntegerType(), False, None)
        )

    sc = (
        T.StructType()
        .add('id', T.IntegerType(), False, None)
        .add('tag', T.StringType(), False, None)
        .add('c1', T.LongType(), False, None)
        .add('c2', T.FloatType(), False, None)
        .add('c3', T.IntegerType(), False, None)
        )

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

    return {
        "dfa" : spark_session.createDataFrame(da, sa),
        "dfb" : spark_session.createDataFrame(db, sb),
        "dfc" : spark_session.createDataFrame(dc, sc)
        }

    