import task1
import task2
import task3
import task4
import task5
import utilities as ut

t_basics = ut.read_to_df("data/t_basics.tsv")
ratings = ut.read_to_df("data/ratings.tsv")
principals = ut.read_to_df("data/principals.tsv")
n_basics = ut.read_to_df("data/n_basics.tsv")
crew = ut.read_to_df("data/crew.tsv")

if __name__ == "__main__":

    sf = ut.standart_filter(t_basics, ratings)

    ut.write_csv(task1.top_all_years(sf), 'top_all_years')
    ut.write_csv(task1.top_ten_years(sf), 'top_ten_years')
    ut.write_csv(task1.top_sixties(sf), 'top_sixties')
    ut.write_csv(task2.genres_top_films(sf), 'genres_top_films')
    ut.write_csv(task3.decade_top_films(sf), 'decade_top_films')
    ut.write_csv(task4.top_actors(sf), 'top_actors')
    ut.write_csv(task5.director_top_films(sf), 'director_top_films')
