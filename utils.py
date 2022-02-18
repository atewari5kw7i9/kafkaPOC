import configparser
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, DateType, StringType
from pyspark.sql.functions import explode, col, split


def load_adobe_df(spark, data_file):

    adobeSchemaStruct = StructType([
        StructField("hit_time_gmt", DateType()),
        StructField("date_time", DateType()),
        StructField("user_agent", StringType()),
        StructField("ip", StringType()),
        StructField("event_list", StringType()),
        StructField("geo_city", StringType()),
        StructField("geo_region", StringType()),
        StructField("geo_country", StringType()),
        StructField("pagename", StringType()),
        StructField("page_url", StringType()),
        StructField("product_list", StringType()),
        StructField("referrer", StringType())
    ])

    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", "\t") \
        .schema(adobeSchemaStruct) \
        .csv(data_file)



def select_adobe_df(adobe_raw_df):
    # return adobe_raw_df.filter("event_list = 1") \
    #     .select("event_list", "product_list", "page_url", "referrer")
    return adobe_raw_df.select("event_list", "product_list", "page_url", "referrer")

def filter_adobe_df(adobe_df):
     print(adobe_df)
     return adobe_df.filter("events_types = 1") \
         .select("events_types", "product_attributes", "page_url", "referrer")

def explode_adobe_df(adobe_df):
    # return adobe_raw_df.filter("event_list = 1") \
    #     .select("event_list", "product_list", "page_url", "referrer")
    adobe_df_tmp = adobe_df.select(explode(adobe_df.product_list_arr).alias("product_attributes"),adobe_df.event_list_arr, adobe_df.page_url, adobe_df.referrer)
    adobe_df_tmp = adobe_df_tmp.select(explode(adobe_df_tmp.event_list_arr).alias("events_types"), adobe_df_tmp.product_attributes, adobe_df_tmp.page_url, adobe_df_tmp.referrer)
    adobe_df_tmp = adobe_df_tmp.withColumn("events_types", col("events_types").cast("int"))
    return adobe_df_tmp

def split_adobe_df(adobe_exploded_df):
    split_cols = split(adobe_exploded_df['product_attributes'], ';')
    df1 = adobe_exploded_df.withColumn('Total_Revenue', split_cols.getItem(3))
    return df1

def scrap_search_url(adobe_df):
    search_str_begin = "(p=|q=|k=)"
    search_str_terminator = "&"
    domain_str_begin="//"
    domain_str_end = "/"

    df1 = adobe_df.withColumn("search1", split(split(adobe_df.referrer, search_str_begin)[1], search_str_terminator)[0])
    df1 = df1.withColumn("domain_name", split(split(adobe_df.referrer, domain_str_begin)[1], domain_str_end)[0])
    df1 = df1.drop("referrer").drop("page_url")
    return df1

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf
