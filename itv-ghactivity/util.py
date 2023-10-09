from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth

from config import Config


def get_spark_session(env, app_name):
    if env == 'DEV':
        master = 'local'
    elif env == 'PROD':
        master = 'yarn'
    spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()
    return spark


def read_files(spark, f_ptrn, cfg: Config):
    df = spark. \
        read. \
        format(cfg.SRC_FMT). \
        load(f'{cfg.SRC_DIR}/{f_ptrn}')
    return df


def to_files(df, cfg: Config):
    df.coalesce(16). \
        write. \
        partitionBy('year', 'month', 'dayofmonth'). \
        mode('append'). \
        format(cfg.TGT_FMT). \
        save(cfg.TGT_DIR)


def transform(df):
    return df.withColumn('year', year('created_at')). \
        withColumn('month', month('created_at')). \
        withColumn('dayofmonth', dayofmonth('created_at'))
