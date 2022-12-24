import json
import math
import time

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from elasticsearch import Elasticsearch
import pyspark.sql.functions as F

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

from kafka import KafkaConsumer

from subprocess import check_output

topic = "openapi-vehicule"
ETH0_IP = check_output(["hostname"]).decode(encoding="utf-8").strip()

elastic_host = "localhost"
elastic_index = "busvehicle"
elastic_document = "_doc"

SPARK_MASTER_URL = "local[*]"
SPARK_DRIVER_HOST = ETH0_IP

spark_conf = SparkConf()
spark_conf.setAll(
    [
        ("spark.master", SPARK_MASTER_URL),
        ("spark.driver.bindAddress", "0.0.0.0"),
        ("spark.driver.host", SPARK_DRIVER_HOST),
        ("spark.app.name", "vehicule-tracking"),
        ("spark.submit.deployMode", "client"),
        ("spark.ui.showConsoleProgress", "true"),
        ("spark.eventLog.enabled", "false"),
        ("spark.logConf", "false"),
    ]
)


if __name__ == "__main__":
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # elasticsearch mapping
    es_mapping = {
        "properties":
        {
	    "actualtime": {"type": "date"},
	    "last_updated_on": {"type": "date"},
	    "speed": {"type": "long"},
	    "call_name": {"type": "long"},
	    "heading": {"type": "long"},
	    #"location": {"type": "geo_point"},
	    "lat": {"type": "long"},
	    "lon": {"type": "long"},
	    "Original_route_id": {"type": "long"},
	    #"routes_served": {"type": "object"},
	    "route_short_name": {"type": "text"},
	    "route_long_name": {"type": "text"},
	    "agencie": {"type": "long"},
	    "transport_type": {"type": "text"},
	    "arr_route_id": {"type": "long"},
	    "arrival_at": {"type": "date"},
	    "arr_stop_id": {"type": "long"},
	    "arr_route_name": {"type": "text"},
	    "delay": {"type": "long"},
	    "geo": {"type": "geo_point"}
	   
        }
    }

    es = Elasticsearch(hosts = "http://localhost:9200/")


    response = es.indices.create(
        index = "busvehicle",
        mappings = es_mapping,
        ignore = 400 # Ignore Error 400 Index already exists
    )
    
    if 'acknowledged' in response:
        if response['acknowledged'] == True:
            print("Index mapping SUCCESS: ", response['index'])

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "vehicle") \
        .option("startingOffsets", "earliest") \
        .option("enable.auto.commit", "true") \
        .load()
    
    df.printSchema()
    schema = (
        StructType()
        .add("actualtime", StringType(), True)
        .add("last_updated_on", StringType(), True)
        .add("speed", FloatType(), True)
        .add("call_name", StringType(), True)
        .add("heading", LongType(), True)
        .add("location", MapType(StringType(), FloatType()), True)
        .add("lat", FloatType(), True)
        .add("lon", FloatType(), True)
        .add("Original_route_id", StringType(), True)
        .add("route_short_name", StringType(), True)
        .add("route_long_name", StringType(), True)
        .add("agencie", StringType(), True)
        .add("transport_type", StringType(), True)
     #   .add("routes_served", ArrayType(), True)
        .add("arr_route_id",  StringType(), True)
        .add("arrival_at", StringType(), True)
        .add("arr_stop_id",  StringType(), True)
        .add("arr_route_name", StringType(), True)
    ) 
    
    
    def func_call(df, batch_id):
     #   df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").select(
     #       F.from_json(F.col("value"), schema).alias('json')
     #   ).select("json.*")
        
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").select(
            from_json(col("value"), schema).alias("vehicule_data")
        ).select(
            "vehicule_data.actualtime",
            "vehicule_data.last_updated_on",
            "vehicule_data.speed",
            "vehicule_data.call_name",
            "vehicule_data.location",
            "vehicule_data.lat",
            "vehicule_data.lon",
            #"vehicule_data.routes_served",
            "vehicule_data.Original_route_id",
            "vehicule_data.route_short_name",
            "vehicule_data.route_long_name",
            "vehicule_data.agencie",
            "vehicule_data.transport_type",
            "vehicule_data.arr_route_id",
            "vehicule_data.arrival_at",
            "vehicule_data.arr_stop_id",
            "vehicule_data.arr_route_name",
        )        
        requests = df.rdd.map(lambda x:  x[1])
        vehicleList = requests.map(lambda x: json.loads(x)).collect() 
        print(vehicleList, "\n")
        
        vehicleDF = spark.createDataFrame(data = vehicleList, schema = schema)
        print(vehicleDF , "\n")
        
        geo_udf = udf(lambda y,x: [x, y], ArrayType(FloatType(),False))
   
        vehicleDF.show(3)
        print(vehicleDF , "\n")
        
        def transform_data(df):
          
          df = df.withColumn('geo', geo_udf(df.lat, df.lon))
          
          # Calculating delay, the difference between actual and scheduled arrival time
          df = df.withColumn('last_updated_on',to_timestamp(col('last_updated_on')))\
          .withColumn('arrival_at', to_timestamp(col('arrival_at')))\
          .withColumn('actualtime', to_timestamp(col('actualtime')))\
          .withColumn('delay',unix_timestamp('arrival_at')-unix_timestamp("actualtime"))

          # adding weekday, month, year, and hour of the day to the table
          df = df.withColumn('weekday', dayofweek('actualtime'))\
          .withColumn('month', month('actualtime'))\
          .withColumn('year', year('actualtime'))\
          .withColumn('time', hour('actualtime'))  

          df = df.withColumn("Original_route_id", col("Original_route_id").cast('bigint'))
          df = df.withColumn("arr_stop_id", col("arr_stop_id").cast('bigint'))
          df = df.withColumn("call_name", col("call_name").cast('bigint'))
          df = df.withColumn('arrival_time', date_format('arrival_at', 'HH:mm:ss'))

          df.show(3)
          return df
        
        vehicleDF = transform_data(vehicleDF)
        print(vehicleDF , "\n")
        
        
        # features specify which features are to be used
        features = ['weekday', 'month', 'time','arr_stop_id', 'speed']

        # the model will aim to predict 'delay', the residual between actual and scheduled arrival time
        label = 'delay'

        # asssembling features to one sparse vector column
        vectorAssembler = VectorAssembler(inputCols = features, outputCol = 'features').setHandleInvalid("skip")
        pipeline = Pipeline(stages = [vectorAssembler])
        non_ohe_df = pipeline.fit(vehicleDF).transform(vehicleDF)
        non_ohe_df = non_ohe_df.select(['features', label])
        for row in non_ohe_df.collect():
          print(row)
        
        #Splitting into train and test data for Random forest regression 
        vector_df = non_ohe_df 

        # splitting the data into training and test
        splits = vector_df.randomSplit([0.7, 0.3])
        train_df = splits[0].cache()
        test_df = splits[1].cache()
  
        (trainingData, testData) = (train_df, test_df)

        # Training of RandomForest model.
        rf = RandomForestRegressor(featuresCol="features", labelCol='delay', maxDepth=10)

        # Chain indexer and forest in a Pipeline
        pipeline = Pipeline(stages=[rf])

        # Train model,  This also runs the indexer.
        model = pipeline.fit(trainingData)

        # Make predictions.
        predictions = model.transform(testData)

        # Select example rows to display.
        predictions.select("prediction", "delay", "features").show(5)

        # Select (prediction, true label) and compute test error
        evaluator = RegressionEvaluator(
         labelCol="delay", predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)
        print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

        rfModel = model.stages[-1]
        print(rfModel) 
          
        vehicleDF.write.format("org.elasticsearch.spark.sql").mode("append").option("checkpointLocation", "/tmp/fben/stream_cp/").option("es.nodes", "http://localhost:9200/").save("busvehicle")
        

    display = df.writeStream.format("console").foreachBatch(func_call).trigger(processingTime=("30 seconds")).start().awaitTermination()
    print("PySpark Structured Streaming with Kafka Application Completed....")