from pyspark.sql import functions as f

from utilities import window


def genres_top_films(df):
    """
    Find top 10 films in each genre
    """
    return df \
        .select('tconst', 'primaryTitle', 'startYear', 'genres', 'averageRating', 'numVotes') \
        .withColumn('row_number', f.row_number().over(window('genres', 'averageRating'))) \
        .where(f.col('row_number') < 11).orderBy(f.col('row_number'))
