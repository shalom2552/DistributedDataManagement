import findspark
import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep

findspark.init()
from pyspark.sql import SparkSession


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


spark, sc = init_spark('demo')


import re
from pyspark.sql import functions as f

# load the data as you did before, just now change the delimiter to get evrey thing together
credits =  spark.read.format("csv")\
  .option("delimiter", "\t")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load("../data/credits.csv")

prog = re.compile('\\[(.*?)\\]')
second_match = f.udf(lambda x: prog.findall(x)[1])
id_extract = f.udf(lambda x: x.split(",")[-1])

credits = credits\
  .withColumn("id", id_extract("cast,crew,id"))\
  .withColumn("cast", f.regexp_extract(f.col("cast,crew,id"), '\\[(.*?)\\]', 0))\
  .withColumn("crew", f.concat(f.lit("["),second_match("cast,crew,id"), f.lit("]")))\
  .select("cast", "crew", "id")

credits.show(5)


def main():
    pass


if __name__ == '__main__':
    print("Part 1")
    main()
