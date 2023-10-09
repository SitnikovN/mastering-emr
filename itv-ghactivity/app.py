import os

from config import Config
from util import (
    get_spark_session,
    read_files,
    transform,
    to_files
)


def main():
    print(os.environ)
    env = os.environ['ENV']
    file_pattern = f"{os.environ['SRC_FILE_PATTERN']}-*"

    app_name = 'GitHub Activity'
    spark = get_spark_session(env, app_name)
    df = read_files(spark, file_pattern, Config)
    df_transformed = transform(df)
    to_files(df_transformed, Config)


if __name__ == '__main__':
    main()
