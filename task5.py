from pyspark.sql import functions as f

from utilities import window, join_table


def director_top_films(df, crew, n_basics):
    """
    Find director's best films
    """

    explode = f.explode(f.split('directors', ','))

    df = df.orderBy(f.col('averageRating').desc(), f.col('numVotes').desc())
    df = join_table(df, crew, 'tconst') \
        .withColumn('directors', explode)
    df = join_table(df, n_basics, df.directors == n_basics.nconst) \
        .withColumn('f_rank', f.dense_rank().over(window('directors', 'averageRating')
                                                  .orderBy(f.col('numVotes').desc())))

    return df.select(
        'primaryName', 'primaryTitle', 'startYear',
        'averageRating', 'numVotes') \
        .where(f.col('f_rank') <= 5) \
        .orderBy(f.col('directors'))
