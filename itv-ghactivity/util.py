from pyspark.sql import SparkSession


def get_spark_session(env, app_name):
    if env == 'DEV':
        master = 'local'
    spark = SparkSession. \
        builder. \
        master(master). \
        appName(app_name). \
        getOrCreate()
    return spark
