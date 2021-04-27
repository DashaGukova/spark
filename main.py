from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Films") \
        .getOrCreate()

    t_basics = spark.read.option("sep", "\t") \
        .csv("data/t_basics.tsv", inferSchema=True, header=True)
    ratings = spark.read.option("sep", "\t") \
        .csv("data/ratings.tsv", inferSchema=True, header=True)

    t_basics.join(ratings, on=["tconst"]) \
        .filter((t_basics.titleType == "movie")
                    & (ratings.numVotes >= 100000)) \
        .select("tconst", t_basics.primaryTitle, ratings.numVotes, ratings.averageRating, t_basics.startYear) \
        .orderBy(ratings.averageRating.desc()) \
        .show(100)

    t_basics.join(ratings, on=["tconst"]) \
        .filter((t_basics.titleType == "movie")
                    & (ratings.numVotes >= 100000)
                    & (t_basics.startYear > 2010)) \
        .select("tconst", t_basics.primaryTitle, ratings.numVotes, ratings.averageRating, t_basics.startYear) \
        .orderBy(ratings.averageRating.desc()) \
        .show(100)

    t_basics.join(ratings, on=["tconst"]) \
        .filter((t_basics.titleType == "movie")
                & (ratings.numVotes >= 100000)
                & ((t_basics.startYear > 1959)
                    & (t_basics.startYear < 1970))) \
        .select("tconst", t_basics.primaryTitle, ratings.numVotes, ratings.averageRating, t_basics.startYear) \
        .orderBy(ratings.averageRating.desc()) \
        .show(100)
