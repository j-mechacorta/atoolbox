from typing import List
from pyspark.sql.dataframe import DataFrame
from pyspark import StorageLevel
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

def toJson(dataframe: DataFrame, file_name:str = "out", format: str= "parquet") -> DataFrame:
    pass

def toJson():
    pass    