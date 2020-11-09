import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.sql.functions import col
from operator import add
from functools import reduce
import json
import atoolbox.spark as tbs


sa = StructType([
    StructField('id', IntegerType(), False),
    StructField('tag', StringType(), False),
    StructField('a1', IntegerType(), False),
    StructField('a2', IntegerType(), False),
    ])

da = [
        (1,'a',1,1),
        (2,'a',1,1),
        (3,'a',1,1)]

spark = (SparkSession.builder
    .config("master", "local[*]")
    .appName("test-session")
    .getOrCreate())

dfa = spark.createDataFrame(da, sa)

def test_export_as_json():
    df = tbs.toJson(dfa)
    isJson = False
    _file = ""
    _walk = os.walk("/tmp/out/")
    for root, dirs, files in _walk:
        _fs = [_f for _f in files if not _f[0] == '.']
        _fs = [_f for _f in _fs if not '_SUCCESS' in _f]
        _file = _fs[0]
    with open(f'/tmp/out/{_file}') as f:
        try:
            data = json.load(f)
            isJson=True
        except IOError as e:
            print(e)
            isJson = False
    
    assert isJson