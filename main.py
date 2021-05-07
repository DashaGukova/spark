from pyspark.sql import SparkSession

import task1
import task2
import task3
import task4
import task5
from utilities import read_to_df, standart_filter, write_csv, explode_genres

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master('local[*]') \
        .appName('Films') \
        .getOrCreate()

    t_basics = read_to_df(spark, 'data/t_basics.tsv')
    ratings = read_to_df(spark, 'data/ratings.tsv')
    principals = read_to_df(spark, 'data/principals.tsv')
    n_basics = read_to_df(spark, 'data/n_basics.tsv')
    crew = read_to_df(spark, 'data/crew.tsv')

    standart_filter_df = standart_filter(t_basics, ratings)
    explode_genres_df = explode_genres(standart_filter_df)

    write_csv(task1.top_all_years(standart_filter_df), 'top_all_years')
    write_csv(task1.top_ten_years(standart_filter_df), 'top_ten_years')
    write_csv(task1.top_sixties(standart_filter_df), 'top_sixties')
    write_csv(task2.genres_top_films(explode_genres_df), 'genres_top_films')
    write_csv(task3.decade_top_films(explode_genres_df), 'decade_top_films')
    write_csv(task4.top_actors(standart_filter_df, principals, n_basics), 'top_actors')
    write_csv(task5.director_top_films(standart_filter_df, crew, n_basics), 'director_top_films')
