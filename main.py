import task1, task2, task3, task4, task5
from pyspark.sql import SparkSession
from connections import write_csv

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Films") \
        .getOrCreate()

    write_csv(task1.top_all_years(), 'top_all_years')
    write_csv(task1.top_ten_years(), 'top_ten_years')
    write_csv(task1.top_sixties(), 'top_sixties')
    write_csv(task2.genres_top_films(), 'genres_top_films')
    write_csv(task3.decade_top_films(final_rating), 'decade_top_films')
    write_csv(task4.top_actors(cast_person), 'top_actors')
    write_csv(task5.director_top_films(crew_person), 'director_top_films')

t_basics = spark.read.option("sep", "\t") \
    .csv("data/t_basics.tsv", inferSchema=True, header=True)
ratings = spark.read.option("sep", "\t") \
    .csv("data/ratings.tsv", inferSchema=True, header=True)
principals = spark.read.option("sep", "\t") \
    .csv('data/principals.tsv', inferSchema=True, header=True)
n_basics = spark.read.option("sep", "\t") \
    .csv('data/n_basics.tsv', inferSchema=True, header=True)
crew = spark.read.option("sep", "\t") \
    .csv('data/crew.tsv', inferSchema=True, header=True)


def standart_filter():
    standart_filter = t_basics.join(ratings, t_basics.tconst == ratings.tconst) \
        .filter((t_basics.titleType == "movie")
                & (ratings.numVotes >= 100000))
    return standart_filter


