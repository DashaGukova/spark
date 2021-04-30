from main import standart_filter, t_basics, ratings
from datetime import datetime


def top_all_years():
    """
    Find top 100 films of all years
    """
    top_all_years = standart_filter \
        .select("tconst", t_basics.primaryTitle, ratings.numVotes, ratings.averageRating, t_basics.startYear) \
        .orderBy(ratings.averageRating.desc())
    return top_all_years


def top_ten_years():
    """
    Find top 100 films in last 10 years
    """
    top_ten_years = standart_filter \
        .filter(t_basics.startYear > (datetime.now().year) - 10) \
        .select("tconst", t_basics.primaryTitle, ratings.numVotes, ratings.averageRating, t_basics.startYear) \
        .orderBy(ratings.averageRating.desc())
    return top_ten_years


def top_sixties():
    """
    Find top 100 films in 60th
    """
    top_sixties = standart_filter \
        .filter((t_basics.startYear > 1959)
                & (t_basics.startYear < 1970)) \
        .select("tconst", t_basics.primaryTitle, ratings.numVotes, ratings.averageRating, t_basics.startYear) \
        .orderBy(ratings.averageRating.desc())
    return top_sixties
