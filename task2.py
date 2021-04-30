from main import standart_filter, t_basics, ratings
from pyspark.sql import functions as f
from pyspark.sql.window import Window

data = t_basics.join(ratings, on=["tconst"])

window = Window.partitionBy('genres') \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc())

explode = f.explode(f.split(data.genres, ',')).alias('genres')


def genres_top_films():
    genres_top_films = standart_filter \
        .select('tconst', 'primaryTitle', 'startYear', explode, 'averageRating', 'numVotes') \
        .withColumn('row_number', f.row_number().over(window)).where(f.col('row_number') < 11)
    return genres_top_films
