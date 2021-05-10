from IPython.core.display import display

from pyspark import SparkContext
from pyspark.sql import *
from urllib.request import urlopen
import json
import pandas as pd
from pyspark.sql.types import *
from pyspark import SparkContext

sc = SparkContext(master="local[2]", appName="readJSON")
sqlContext = SQLContext(sc)


# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]':
        return TimestampType()
    elif f == 'int64':
        return LongType()
    elif f == 'int32':
        return IntegerType()
    elif f == 'float64':
        return FloatType()
    else:
        return StringType()


def define_structure(string, format_type):
    try:
        typo = equivalent_type(format_type)
    except:
        typo = StringType()
    return StructField(string, typo)


# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types):
        struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)


response = urlopen(
    "http://cf-code-challenge-40ziu6ep60m9.s3-website.eu-central-1.amazonaws.com/ohlcv-btc-usd-history-6min-2020.json")
json_data = response.read().decode('utf-8', 'replace')

d = json.loads(json_data)
df = pd.json_normalize(d)
# convert pandas dataframe to spark dataframe
spark_df = pandas_to_spark(df)
# store in a table tbl_Price
spark_df.createOrReplaceTempView("tbl_Price")
df.head()
# fetch the standard deviation along with other metrics and store it in a table STD_price
res = df.describe()
display(res)
spark_df1 = pandas_to_spark(res)
spark_df1.registerTempTable("STD_price")

# spark_df.write.mode("overwrite").saveAsTable("test_table2")
# spark.sql("select * from test_table2")
