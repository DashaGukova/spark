from pyspark.sql import functions as f

from utilities import join_table


def top_actors(df, principals, n_basics):
    """
    Find actors
    """
    df = df.orderBy(f.col('averageRating').desc(), f.col('numVotes').desc())
    df = join_table(df, principals, 'tconst')
    df = join_table(df, n_basics, 'nconst').where(f.col('category').like('act%'))

    return df \
        .groupby('nconst', 'primaryName').count() \
        .select('primaryName') \
        .orderBy(f.col('count').desc())
