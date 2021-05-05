from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as f


spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Films") \
        .getOrCreate()


def window(column):
    """
    Make standart window function
    """
    return Window.partitionBy(column)\
        .orderBy(f.col('averageRating').desc(),
                 f.col('numVotes').desc())


def join_table(left_df, right_df, condition, how='inner'):
    """
    Join two datasets
    """
    return left_df.join(right_df, condition, how)


def standart_filter(left_df, right_df):
    """
    Make join and filter by numVotes, titleType
    """
    return left_df.join(right_df, left_df.tconst == right_df.tconst) \
        .drop(right_df.tconst)        \
        .filter((left_df.titleType == 'movie')
                & (right_df.numVotes >= 100000))


def read_to_df(path):
    """
    Read dataframe
    """
    return spark.read.option('sep', '\t').csv(path, inferSchema=True, header=True)


def write_csv(data_frame, file_name):
    """
    Write dataframe into csv
    """
    data_frame.coalesce(1).write \
        .option('header', True).mode('overwrite') \
        .save(f'outputs/{file_name}', format('csv'))
