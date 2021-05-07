from pyspark.sql import functions as f

from utilities import window, year_range


def decade_top_films(df):
    """
    Find top 10 films in every genre for decades since 1950
    """

    df = df.where(f.col('startYear') >= 1950)
    df = year_range(df) \
        .orderBy(f.col('averageRating').desc(), f.col('numVotes').desc()) \
        .withColumn('g_rank', f.dense_rank().over(window('genres', 'averageRating')
                                                  .orderBy(f.col('numVotes').desc()))) \
        .withColumn('yearRange', f.dense_rank().over(window('year_range', 'year_range')))

    return df.where(df.g_rank <= 10)\
        .select('tconst', 'primaryTitle', 'startYear', 'genres', 'averageRating', 'numVotes', 'yearRange') \
        .orderBy(f.col('yearRange').desc(), f.col('genres'), f.col('g_rank'))
