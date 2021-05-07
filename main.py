from pyspark.sql import SparkSession

import task1
import task2
import task3
import task4
import task5
from utilities import read_to_df, standart_filter, write_csv, with_column, explode

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Films") \
        .getOrCreate()

    t_basics = read_to_df(spark, "data/t_basics.tsv")
    ratings = read_to_df(spark, "data/ratings.tsv")
    principals = read_to_df(spark, "data/principals.tsv")
    n_basics = read_to_df(spark, "data/n_basics.tsv")
    crew = read_to_df(spark, "data/crew.tsv")

    sf = standart_filter(t_basics, ratings)
    wc = with_column(sf, 'genres', explode('genres'))

    write_csv(task1.top_all_years(sf), 'top_all_years')
    write_csv(task1.top_ten_years(sf), 'top_ten_years')
    write_csv(task1.top_sixties(sf), 'top_sixties')
    write_csv(task2.genres_top_films(wc), 'genres_top_films')
    write_csv(task3.decade_top_films(wc), 'decade_top_films')
    write_csv(task4.top_actors(sf, principals, n_basics), 'top_actors')
    write_csv(task5.director_top_films(sf, crew, n_basics), 'director_top_films')
