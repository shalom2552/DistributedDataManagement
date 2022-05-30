import findspark

findspark.init()
from pyspark.sql import SparkSession


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


if __name__ == '__main__':
    DATE, VALUE, FACE, CURTAIN = 0, 1, 2, 3

    spark, sc = init_spark('demo')
    data_file = 'data_HW1.csv'
    curtains_rdd = sc.textFile(data_file)
    csv_rdd = curtains_rdd.map(lambda row: row.split(','))
    header = csv_rdd.first()
    data_rdd = csv_rdd.filter(lambda row: row != header)

    data_rdd = data_rdd.map(
        lambda row: ((row[DATE].split('/')[0], int(row[DATE].split('/')[1]), row[FACE], row[CURTAIN]), (row[VALUE], 1)))
    data_rdd = data_rdd.reduceByKey(lambda a, v: [float(a[0]) + float(v[0]), a[1] + v[1]]).mapValues(
        lambda x: x[0] / x[1])

    invalid_records = data_rdd.filter(lambda x: x[1] < 0.05).map(lambda x: (x[0][-3], x[0][-2])).distinct()

    results = data_rdd.map(lambda x: (x[0][-3], x[0][-2])).distinct().subtract(invalid_records)

    month = results.map(lambda x: x[-2]).distinct().sortBy(lambda x: x)

    for mon in month.take(month.count()):
        res = []
        for record in results.take(results.count()):
            if record[0] == mon:
                res.append(record[1])
        print(str(mon) + ": " + str(sorted(res)))
