from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
import os

from pyspark.ml.linalg import VectorUDT
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

SCHEMA = StructType([StructField("Arrival_Time",LongType(),True),
                     StructField("Creation_Time",LongType(),True),
                     StructField("Device",StringType(),True),
                     StructField("Index", LongType(), True),
                     StructField("Model", StringType(), True),
                     StructField("User", StringType(), True),
                     StructField("gt", StringType(), True),
                     StructField("x", DoubleType(), True),
                     StructField("y", DoubleType(), True),
                     StructField("z", DoubleType(), True)])

spark = SparkSession.builder.appName('demo_app')\
    .config("spark.kryoserializer.buffer.max", "512m")\
    .getOrCreate()

os.environ['PYSPARK_SUBMIT_ARGS'] = \
    "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8,com.microsoft.azure:spark-mssql-connector:1.0.1"
kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
topic = "activities"

# create the global model and the global df 
static_schema = StructType([StructField("label", FloatType(),True),
                            StructField("features", VectorUDT(),True)])
unbounded_df =  spark.createDataFrame(data = spark.sparkContext.emptyRDD(), schema = static_schema) 
correct = 0  
total = 0 
model = 0
flag = 1
stop_requirment = 55 #  546,083 rows in the data, 10,000 maxoffsetpertrigger will lead into 55 batch calls

streaming = spark.readStream\
                  .format("kafka")\
                  .option("kafka.bootstrap.servers", kafka_server)\
                  .option("subscribe", topic)\
                  .option("startingOffsets", "earliest")\
                  .option("failOnDataLoss",False)\
                  .option("maxOffsetsPerTrigger", 100000)\
                  .load()\
                  .select(f.from_json(f.decode("value", "US-ASCII"), schema=SCHEMA).alias("value")).select("value.*")


def foreach_batch(df, epoch_id):
  global unbounded_df
  global correct
  global total
  global model
  global flag

  # string indexer that works in spark 2.4
  gt_ = ['null', 'stairsup', 'walk', 'stand', 'sit', 'bike', 'stairsdown']
  gt_index = [str(num) for num in range(len(gt_))]
  gt_dict = dict(zip(gt_, gt_index))

  # Manual string indexer for the model column containing one variable
  df = df.withColumn('Model_Index', f.lit(0.0))

  # Manual string indexer for the 'Device' column
  device_ = ["nexus4_1", "nexus4_2"]
  index_device = [str(num) for num in range(len(device_))]
  devicedict = dict(zip(device_, index_device))
      
  df = df.withColumn("Device_Index", f.col("Device")).replace(to_replace=devicedict, subset=["Device_Index"])\
      .withColumn("Device_Index", f.col("Device_Index").cast(FloatType()))

  
  df = df.withColumn("label", f.col("gt")).replace(to_replace=gt_dict, subset=["label"])\
    .withColumn("label", f.col("label").cast(FloatType()))\
                .select('Arrival_Time','Creation_Time','x','y', 'z','Device_index','Model_index','label','User')

  
  # string indexer that works in spark 2.4
  users_ = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i']
  user_columns = [f.when(f.col("User") == uid, 1).otherwise(0).alias("User_ID_" + uid) for uid in users_]
  user_fields = ['User_ID_' + str(i) for i in users_]

  df = df.select(*user_columns,'Arrival_Time','Creation_Time','x','y','z','Device_index','Model_index','label','User')

  vector_columns = ['Arrival_Time','Creation_Time','x','y',
                    'z','Device_index','Model_index'] + user_fields
  assembler = VectorAssembler(inputCols=vector_columns, outputCol='features')
  df = assembler.transform(df).select('label', 'features')  # in this part the batch data is now ready for prediction

  test_data = df  
  
  if total == 0: # first prediction - according to the mail shay sent we can predict the first batch after train him
    rfClassifier = RandomForestClassifier(numTrees=5, maxDepth=30, maxBins =70)
    print("check")
    model = rfClassifier.fit(df)
    test_predicted = model.transform(df)
    batch_correct = test_predicted.filter(f.col("prediction") == f.col("label")).count()
    batch_rows = test_predicted.count()
    
    
  else: # not first batch so we use the global model to get a prediction
    test_predicted = model.transform(test_data)
    
    batch_correct = test_predicted.filter(f.col("prediction") == f.col("label")).count()
    batch_rows = test_predicted.count()
    
  correct += batch_correct
  total += batch_rows
    
  print("(Current Batch:",epoch_id + 1, ") Current Accuracy: ",round(correct/total*100, 2),"% --- ( the total predicted: ", total, ", the total correct: ",correct, ", correct in current batch: ", batch_correct, ")")
  
  if total % 100000 != 0:
    print("\nNO MORE STREAMD DATA, FINAL AND TOTAL ACCURACY IS :", round(correct/total*100, 2),"%---")
    flag = 0
  elif (epoch_id + 1 ) < 40:
    # create RandomForestClassifier
    rfClassifier = RandomForestClassifier(numTrees=5, maxDepth=30, maxBins =70)
    
    # update unbounded_df that works in spark 2.4
    unbounded_df = unbounded_df.union(df)  
    
    # train model for the next batch
    model = rfClassifier.fit(unbounded_df)

stream_obj = streaming.writeStream.trigger(processingTime='4 seconds').foreachBatch(foreach_batch).start()
while(flag):
  pass
stream_obj.stop()