from pyspark.sql import functions as f

import main as m
import utilities as ut
from utilities import window


def director_top_films(df):
    """
    Find director's best films
    """

    explode = f.explode(f.split('directors', ','))

    df.drop(m.t_basics.tconst).orderBy(f.col('averageRating').desc(), f.col('numVotes').desc())
    df = ut.join_table(df, m.crew, 'tconst')\
        .drop(m.crew.tconst)\
        .withColumn('director', explode)
    df = ut.join_table(df, m.n_basics, df.director == m.n_basics.nconst)\
        .drop(m.n_basics.nconst)\
        .withColumn('f_rank', f.dense_rank().over(window('director')))

    return df.select(
        'primaryName', 'primaryTitle', 'startYear',
        'averageRating', 'numVotes') \
        .where(f.col('f_rank') <= 5) \
        .orderBy(f.col('director'))
