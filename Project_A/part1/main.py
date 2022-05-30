import re
import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def main():
    users, queries = parse_users('users.csv'), parse_queries('queries.csv')
    movies, tickets, credit = parse_movies('movies.csv'), parse_tickets('tickets.csv'), parse_credits('credits.csv')
    users.show()  # example
    pass


def parse_users(path):
    """
    load the .csv file from the path
    parse the data using spark
    cast numbers columns from string to int format type
    cast string to list when needed and get substrings from long strings
    :return: the data frame after transformations
    """
    # load Users
    users = spark.read.option("Header", True).option("multiline", "true").option("escape", "\"").csv(path)
    users = users.withColumn("id", f.col("user_id").cast('int')).drop('user_id')
    users = users.select('id', 'user_location')
    return users


def parse_tickets(path):
    """
    load the .csv file from the path
    parse the data using spark
    cast numbers columns from string to int format type
    cast string to list when needed and get substrings from long strings
    :return: the data frame after transformations
    """
    tickets = spark.read.option("Header", True).option("multiline", "true").option("escape", "\"").csv(path)

    tickets = tickets.withColumn("user_id", f.col("user_id").cast('int'))
    tickets = tickets.withColumn("movie_id", f.col("movie_id").cast('int'))
    tickets = tickets.withColumn("n_tickets", f.col("number_of_tickets").cast('int'))

    tickets = tickets.select('user_id', 'movie_id', 'n_tickets', 'city')
    return tickets


def parse_queries(path):
    """
    load the .csv file from the path
    parse the data using spark
    cast numbers columns from string to int format type
    cast string to list when needed and get substrings from long strings
    :return: the data frame after transformations
    """
    # load queries
    queries = spark.read.option("Header", True).option("multiline", "true").option("escape", "\"").csv(path)
    queries = queries.withColumnRenamed('from_realese_date', 'date').withColumnRenamed('production_company', 'company')

    # cast id to int
    queries = queries.withColumn("id", f.col("user_id").cast('int')).drop('user_id')

    feathers = ['genres', 'lang', 'actors', 'director', 'cities', 'country', 'date', 'company']
    for feather in feathers:
        for special_char in ['\[', '\]', '\'']:
            queries = queries.withColumn(feather, f.regexp_replace(feather, special_char, ''))
            if feather != 'date':
                queries = queries.withColumn(feather, f.split(f.col(feather), ", "))
            else:
                queries = queries.withColumn(feather, f.col(feather).cast('int'))

    # select columns
    queries = queries.select('id', 'genres', 'lang', 'actors', 'director', 'cities', 'country', 'date', 'company')
    return queries


def parse_movies(path):
    """
    load the .csv file from the path
    parse the data using spark
    cast numbers columns from string to int format type
    cast string to list when needed and get substrings from long strings
    :return: the data frame after transformations
    """
    # load Movies
    movies = spark.read.option("Header", True).option("multiline", "true").option("escape", "\"").csv(path)
    # columns select: id, genres, prod_comp, countries, release_date, languages, cities
    movies = movies.select('movie_id', 'genres', 'production_companies', 'production_countries', 'release_date',
                           'spoken_languages', 'cities')
    movies = movies.withColumnRenamed('movie_id', 'id').withColumnRenamed('production_companies','prod_comp')\
        .withColumnRenamed('production_countries', 'countries').withColumnRenamed('spoken_languages', 'languages')
    movies = movies.withColumn("id", f.col("id").cast('int'))

    feathers = ['genres', 'prod_comp', 'countries', 'languages']
    for feather in feathers:
        word = 'name' if feather != 'languages' else 'iso_639_1'
        if feather in ['prod_comp', 'countries']:
            movies = movies.withColumn(feather, f.regexp_replace(feather, ' ', ''))
        movies = movies.withColumn(feather, f.regexp_replace(feather, '\W+', ' '))
        movies = movies.withColumn("ArrayOfWords", f.split(feather, " ")) \
            .withColumn("tmp", f.expr(f"""filter(transform(ArrayOfWords,(x,e)-> 
        CASE WHEN x in ({word}) 
        THEN array_join(slice(ArrayOfWords,e+2,1),' ') ELSE NULL END)
        ,y-> y is not NULL)""")).drop("ArrayOfWords").drop(feather)
        movies = movies.withColumnRenamed('tmp', feather)

    # release date format type
    movies = movies.withColumn("release_date", f.regexp_replace("release_date", '/', '-')
                               .to_date(f.col("release_date"), "dd-MM-yyyy"))
    for special_char in ['\[', '\]', '\'']:
        movies = movies.withColumn('cities', f.regexp_replace('cities', special_char, ''))
    movies = movies.withColumn('cities', f.split(f.col('cities'), ", "))
    return movies


def parse_credits(path):
    """
    load the .csv file from the path
    parse the data using spark
    cast numbers columns from string to int format type
    cast string to list when needed and get substrings from long strings
    :return: the data frame after transformations
    """
    credits_df = spark.read.format("csv").option("delimiter", "\t").option("header", "true")\
        .option("inferSchema", "true").load(path)

    prog = re.compile('\\[(.*?)\\]')
    second_match = f.udf(lambda x: prog.findall(x)[1])
    id_extract = f.udf(lambda x: x.split(",")[-1])
    credits_df = credits_df.withColumn("id", id_extract("cast,crew,id")) \
        .withColumn("cast", f.regexp_extract(f.col("cast,crew,id"), '\\[(.*?)\\]', 0)) \
        .withColumn("crew", f.concat(f.lit("["), second_match("cast,crew,id"), f.lit("]"))) \
        .select("cast", "crew", "id")
    # cast 'id' to integer
    credits_df = credits_df.withColumn("id", f.col("id").cast('int'))
    
    # actors
    to_remove = ["'cast_id': ", '[0-9]', "\W+", "'character': ", ":", ",", " \(voice\)", "'credit_id': "]
    for string in to_remove:
        credits_df = credits_df.withColumn("cast", f.regexp_replace("cast", string, ' '))
    credits_df = credits_df.withColumn("ArrayOfWords", f.split("cast", " ")) \
        .withColumn("actors", f.expr(f"""filter(transform(ArrayOfWords,(x,e)-> 
    CASE WHEN x in ('name') 
    THEN array_join(slice(ArrayOfWords,e+2,2),' ') ELSE NULL END)
    ,y-> y is not NULL)""")).drop("ArrayOfWords").drop("cast")

    # directors
    credits_df = credits_df.withColumn("crew", f.regexp_replace("crew", "'Director'", 'WilNeverBrAgain')).withColumn(
        "crew", f.regexp_replace("crew", "\W+", ' ')).withColumn("crew", f.regexp_replace("crew", " name", ''))
    credits_df = credits_df.withColumn("ArrayOfWords", f.split("crew", " ")) \
        .withColumn("director", f.expr(f"""filter(transform(ArrayOfWords,(x,e)-> 
    CASE WHEN x in ('WilNeverBrAgain') 
    THEN array_join(slice(ArrayOfWords,e+2,2),' ') ELSE NULL END)
    ,y-> y is not NULL)""")).drop("ArrayOfWords").drop("crew")
    return credits_df


if __name__ == '__main__':
    findspark.init()
    spark = SparkSession.builder.appName('Movies.corp').getOrCreate()
    sc = spark.sparkContext
    main()
