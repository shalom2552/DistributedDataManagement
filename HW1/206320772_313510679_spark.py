import findspark

findspark.init()
from pyspark.sql import SparkSession

DATE, VALUE, FACE, CURTAIN = 0, 1, 2, 3


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def main():
    spark, sc = init_spark('demo')
    data_file = './data_HW1.csv'
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

    res = []
    faces8 = []
    faces9 = []
    faces10 = []

    for record in results.take(results.count()):
        if record[0] == 8:
            faces8.append(record[1])
        elif record[0] == 9:
            faces9.append(record[1])
        elif record[0] == 10:
            faces10.append(record[1])
    res.append([8, faces8])
    res.append([9, faces9])
    res.append([10, faces10])
    print(str(res[0][0]) + ", " + str(sorted(res[0][1])))
    print(str(res[1][0]) + ", " + str(sorted(res[1][1])))
    print(str(res[2][0]) + ", " + str(sorted(res[2][1])))
    pass


if __name__ == '__main__':
    main()
