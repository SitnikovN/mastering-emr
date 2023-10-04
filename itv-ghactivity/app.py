import os
from util import get_spark_session


def main():
    env = os.environ['ENV']
    app_name = 'GitHub Activity'
    spark = get_spark_session(env, app_name)
    spark.sql('SELECT current_date').show()


if __name__ == '__main__':
    main()
