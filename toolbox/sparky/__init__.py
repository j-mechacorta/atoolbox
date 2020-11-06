from typing import List
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, collect_list, struct, to_json
from pyspark.sql.types import FloatType, IntegerType, LongType, StringType
from pyspark import StorageLevel
from operator import add
from functools import reduce

def join_dataframes(dataframes: List[DataFrame], on: str, how: str = "left", persist: bool = False) -> DataFrame:
    '''
    
    Join multiple Spark dataframes which contain the same column.


    Parameters
    ----------

    dataframes: List[DataFrame]
        List of Spark dataframes to be joined
    
    on: str
        Column name on which to join all the dataframes
    
    how: str ('left' is the default)
        which join is to be applied to all join operations

    persist: bool
        set to true for persisting dataframe on memory 
    
    Returns
    -------

    DataFrame: Spark Dataframe

    '''
    df = reduce(lambda a,b: a.join(b, on=on, how=how), dataframes)
    if persist:
        df.persist(StorageLevel.MEMORY_ONLY).count()

    return df

def toJson(dataframe: DataFrame, file_name:str = "out", format: str = "text", mode:str = "overwrite") -> DataFrame:
    '''[summary]

    Args:
        dataframe (DataFrame): [description]
        file_name (str, optional): [description]. Defaults to "out".
        format (str, optional): [description]. Defaults to "parquet".

    Returns:
        DataFrame: [description]
    '''
    df2 = dataframe.select(to_json(struct(*dataframe.columns)).alias("json"))
    df3 = df2.agg(collect_list("json").cast(StringType()).alias("json_list"))
    df3.write.mode(mode).format(format).save(f"file:////tmp/out")
    return df3

def add_numeric_fields(dataframe:DataFrame, pivot:str = "id") -> DataFrame:
    '''[summary]

    Parameters
    ----------
    dataframe : DataFrame
        [description]
    pivot : str, optional
        [description], by default "id"

    Returns
    -------
    DataFrame
        [description]
    '''
    dataframe.columns
    fields = dataframe.drop('id').schema.fields
    cols = [x.name for x in fields if x.dataType in [IntegerType(), FloatType(), LongType()]]
    return dataframe.drop('tag').select(pivot, reduce(add, [col(x) for x in cols]).alias('total'))
