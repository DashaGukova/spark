from pyspark.sql import functions as f

from utilities import window


def decade_top_films(df):
    """
    Find top 10 films in every genre for decades since 1950
    """

    df = df \
        .withColumn('genres', f.explode(f.split('genres', ','))) \
        .withColumn('decades', (f.floor(f.col('startYear') / 10) * 10)) \
        .orderBy(f.col('averageRating').desc(), f.col('numVotes').desc()) \
        .withColumn('g_rank', f.dense_rank().over(window('genres', 'averageRating')
                                                  .orderBy(f.col('numVotes').desc()))) \
        .withColumn('d_rank', f.dense_rank().over(window('decades', 'decades'))) \

    return df.where(df.g_rank <= 10)\
        .orderBy(f.col('decades').desc(), f.col('genres'), f.col('g_rank')) \
        .select('tconst', 'primaryTitle', 'startYear', 'genres', 'averageRating', 'numVotes', 'decades')
