from main import standart_filter, t_basics, principals, n_basics
from pyspark.sql import functions as f

t_rating = standart_filter() \
    .drop(t_basics.tconst) \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc())

t_cast = t_rating \
    .join(principals,
          t_rating.tconst == principals.tconst,
          'inner') \
    .drop(principals.tconst)

cast_person = t_cast \
    .join(n_basics,
          t_cast.nconst == n_basics.nconst,
          'inner') \
    .drop(n_basics.nconst) \
    .where(f.col('category').like('act%'))


def top_actors():
    """
    Find actors
    """
    top_actors = cast_person \
        .groupby('nconst', 'primaryName').count() \
        .select('primaryName') \
        .orderBy(f.col('count').desc())

    return top_actors
