from main import standart_filter, t_basics, n_basics, crew
from pyspark.sql import functions as f
from pyspark.sql.window import Window

window = Window.partitionBy('director') \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc())

explode = f.explode(f.split('directors', ','))

t_rating = standart_filter() \
    .drop(t_basics.tconst) \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc())

t_crew = t_rating \
    .join(crew,
          t_rating.tconst == crew.tconst) \
    .drop(crew.tconst) \
    .withColumn('director', explode)

crew_person = t_crew \
    .join(n_basics,
          t_crew.director == n_basics.nconst) \
    .drop(n_basics.nconst) \
    .withColumn('f_rank', f.dense_rank().over(window))


def director_top_films():
    """
    Find director's best films
    """
    director_top_films = crew_person.select(
        'primaryName', 'primaryTitle', 'startYear',
        'averageRating', 'numVotes') \
        .where(f.col('f_rank') <= 5) \
        .orderBy(f.col('director'))

    return director_top_films
