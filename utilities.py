from pyspark.sql.window import Window
from pyspark.sql import functions as f


def explode_genres(df):
    """
    Make explode genres
    """
    return df.withColumn('genres', explode('genres'))


def explode(column_use):
    """
    Explode column by one which contains many strings
    """
    return f.explode(f.split(f.col(column_use), ','))


def window(column, factor_first):
    """
    Make standart window function
    """
    return Window.partitionBy(column) \
        .orderBy(f.col(factor_first).desc())


def join_table(left_df, right_df, condition, how='inner'):
    """
    Join two datasets
    """
    return left_df.join(right_df, condition, how)


def standart_filter(left_df, right_df):
    """
    Make join and filter by numVotes, titleType
    """
    return left_df.join(right_df, 'tconst') \
        .filter((f.col('titleType') == 'movie') & (f.col('numVotes') >= 100000))


def read_to_df(spark, path):
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
