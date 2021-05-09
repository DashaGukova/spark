from pyspark.sql import functions as f
from pyspark.sql.window import Window


def decade_top_films(df):
    """
    Find top 10 films in every genre for decades since 1950
    """
    win = Window.partitionBy('year_range', 'genres').orderBy(f.col('averageRating').desc())

    df = df.where(f.col('startYear') >= 1950)
    decade = (f.col('startYear') - f.col('startYear') % 10).cast('int')
    df = df.withColumn('year_range', f.concat(decade, f.lit('-'), decade + 10)) \
        .withColumn('g_rank', f.row_number().over(win)) \
        .where(f.col('g_rank') <= 10) \
        .orderBy(f.col('year_range').desc(), f.col('genres'), f.col('averageRating').desc()) \
        .select('tconst', 'primaryTitle', 'startYear', 'genres', 'averageRating', 'numVotes', 'year_range')
    return df
