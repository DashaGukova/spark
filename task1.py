from datetime import datetime

from pyspark.sql import functions as f


def top_all_years(df):
    """
    Find top 100 films of all years
    """
    return df \
        .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear') \
        .orderBy(f.col('averageRating').desc())


def top_ten_years(df):
    """
    Find top 100 films in last 10 years
    """
    return df \
        .filter(f.col('startYear') > datetime.now().year - 10) \
        .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear') \
        .orderBy(f.col('averageRating').desc())


def top_sixties(df):
    """
    Find top 100 films in 60th
    """
    return df \
        .filter((f.col('startYear') > 1959) & (f.col('startYear') < 1970)) \
        .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear') \
        .orderBy(f.col('averageRating').desc())
