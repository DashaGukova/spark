from main import standart_filter, t_basics, ratings
from pyspark.sql import functions as f
from pyspark.sql.window import Window

g_window = Window.partitionBy('genres') \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc())

d_window = Window.partitionBy('decades') \
    .orderBy(f.col('decades').desc())
data = t_basics.join(ratings, on=["tconst"])

final_rating = standart_filter.withColumn('genres', f.explode(f.split('genres', ','))) \
    .withColumn('decades', (f.floor(f.col('startYear') / 10) * 10)) \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc()) \
    .withColumn('g_rank', f.dense_rank().over(g_window)) \
    .withColumn('d_rank', f.dense_rank().over(d_window))


def decade_top_films():
    decade_top_films = final_rating.select(
        'tconst', 'primaryTitle', 'startYear',
        'genres', 'averageRating', 'numVotes', 'decades') \
        .where(final_rating.g_rank <= 10) \
        .orderBy(f.col('decades').desc(), f.col('genres'),
                 f.col('g_rank'))
    return decade_top_films
