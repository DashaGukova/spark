from pyspark.sql import SparkSession

from task1 import top_sixties, top_all_years, top_ten_years
from task2 import genres_top_films
from task3 import decade_top_films
from task4 import top_actors
from task5 import director_top_films
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

    # write_csv(top_all_years(standart_filter_df), 'top_all_years')
    # write_csv(top_ten_years(standart_filter_df), 'top_ten_years')
    # write_csv(top_sixties(standart_filter_df), 'top_sixties')
    # write_csv(genres_top_films(explode_genres_df), 'genres_top_films')
    write_csv(decade_top_films(explode_genres_df), 'decade_top_films')
    # write_csv(top_actors(standart_filter_df, principals, n_basics), 'top_actors')
    # write_csv(director_top_films(standart_filter_df, crew, n_basics), 'director_top_films')
