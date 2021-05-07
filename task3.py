from pyspark.sql import functions as f

from utilities import window


def decade_top_films(df):
    """
    Find top 10 films in every genre for decades since 1950
    """

    df = df.where(f.col('startYear') >= 1950)
    decade = (f.col('startYear') - f.col('startYear') % 10).cast('int')
    df = df.withColumn('year_range', f.concat(decade, f.lit('-'), decade + 10)) \
        .orderBy(f.col('averageRating').desc(), f.col('numVotes').desc()) \
        .withColumn('g_rank', f.row_number().over(window('genres', 'averageRating')
                                                  .orderBy(f.col('numVotes').desc()))) \
        .withColumn('yearRange', f.row_number().over(window('year_range', 'year_range')))

    return df.where(df.g_rank <= 10)\
        .select('tconst', 'primaryTitle', 'startYear', 'genres', 'averageRating', 'numVotes', 'year_range') \
        .orderBy(f.col('yearRange').desc(), f.col('genres'), f.col('g_rank'))
