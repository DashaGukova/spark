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
        .filter((ratings.numVotes >= 100000)) \
        .select("tconst", t_basics.primaryTitle
                , t_basics.startYear, t_basics.genres
                , ratings.averageRating, ratings.numVotes) \
        .orderBy(t_basics.genres, ratings.averageRating.desc()) \
        .groupBy([t_basics.genres])\
        .show(10)
