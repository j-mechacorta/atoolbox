import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.sql.functions import col
from operator import add
from functools import reduce
import json
import atoolbox.spark.functions as tbs


def test_export_as_json(test_data):
    df = tbs.toJson(test_data["dfa"])
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