from pyspark.sql import functions as f

import main as m
from utilities import window


def genres_top_films(df):
    """
    Find top 10 films in each genre
    """

    explode = f.explode(f.split(f.col('genres'), ','))

    return df \
        .select('tconst', 'primaryTitle', 'startYear', 'genres', 'averageRating', 'numVotes') \
        .withColumn('genres', explode) \
        .withColumn('row_number', f.row_number().over(window('genres'))).where(f.col('row_number') < 11)
