from pyspark.sql import functions as f
from pyspark.sql.window import Window

from utilities import window as g_window


def decade_top_films(df):
    """
    Find top 10 films in every genre for decades since 1950
    """

    d_window = Window.partitionBy('decades') \
        .orderBy(f.col('decades').desc())

    df = df \
        .withColumn('genres', f.explode(f.split('genres', ','))) \
        .withColumn('decades', (f.floor(f.col('startYear') / 10) * 10)) \
        .orderBy(f.col('averageRating').desc(), f.col('numVotes').desc()) \
        .withColumn('g_rank', f.dense_rank().over(g_window('genres'))) \
        .withColumn('d_rank', f.dense_rank().over(d_window))

    return df\
        .select('tconst', 'primaryTitle', 'startYear', 'genres', 'averageRating', 'numVotes', 'decades') \
        .where(df.g_rank <= 10) \
        .orderBy(f.col('decades').desc(), f.col('genres'), f.col('g_rank'))
