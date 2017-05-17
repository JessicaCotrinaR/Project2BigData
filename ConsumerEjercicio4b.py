from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from operator import add
import sys
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from pyspark.sql import Row, SparkSession
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar pyspark-shell'
try:
    import json
except ImportError:
    import simplejson as json


def read_credentials():
    file_name = "/root/bigdata/sstream02/sample-credentials.json"
    try:
        with open(file_name) as data_file:
            return json.load(data_file)
    except:
        print ("Cannot load credentials.json")
        return None

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def consumer():

    context = StreamingContext(sc, 10)
    dStream = KafkaUtils.createDirectStream(context, ["test"], {"metadata.broker.list": "localhost:9092"})

    #Start Question 1
    dStream.foreachRDD(p1)
    #End Question 1

    context.start()
    context.awaitTermination()

def p1(time,rdd):

    rdd=rdd.map(lambda x: json.loads(x[1]))
    records=rdd.collect()

   # records = [element for element in records if "delete" not in element] #remove delete tweets

    records = [element["text"] for element in records if "text" in element]
    if records:
        rdd = sc.parallelize(records)
        rdd = rdd.map(lambda x :x.split(" "))
        #para  que  las palabras se puedan filtrar
        rdd = rdd.flatMap(lambda x:x)
        rdd = rdd.filter(lambda x: x.startswith(('@','#','RT','--','https','"',':')) == False )
        rdd = rdd.filter(lambda x: len(x) > 2 )

        for r in rdd.collect():
# en ese print muestra spliteado y filtrado
            print (r)
# conectando a hive
        spark = getSparkSessionInstance(rdd.context.getConf())
        # Convert RDD[String] to RDD[Row] to DataFrame
        keywordDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(keyword=x)))
        keywordDataFrame.createOrReplaceTempView("keywords")
        keywordDataFrame = spark.sql("select keyword, count(*) as total, current_timestamp() as timestamp from keywords group by keyword order by total desc limit 3")
        keywordDataFrame.show()
        keywordDataFrame.write.mode("append").saveAsTable("keywordt1")


if __name__ == "__main__":
    print("Stating to read tweets")
    credentials = read_credentials() 
    oauth = OAuth(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'], credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    twitter_stream = TwitterStream(auth=oauth)
    sc = SparkContext(appName="Project 2")
    checkpointDirectory = "/checkpoint"
    consumer()
    
