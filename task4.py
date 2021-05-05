from pyspark.sql import functions as f

import main as m
import utilities as ut


def top_actors(df):
    """
    Find actors
    """
    df.orderBy(f.col('averageRating').desc(), f.col('numVotes').desc())
    df = ut.join_table(df, m.principals, 'tconst').drop(m.principals.tconst)
    df = ut.join_table(df, m.n_basics, 'nconst').drop(m.n_basics.nconst).where(f.col('category').like('act%'))

    return df \
        .groupby('nconst', 'primaryName').count() \
        .select('primaryName') \
        .orderBy(f.col('count').desc())
