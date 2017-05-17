rom pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from operator import add
import sys
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from pyspark.sql import Row, SparkSession
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
    #context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)
    context = StreamingContext(sc, 10)
    dStream = KafkaUtils.createDirectStream(context, ["twitter"], {"metadata.broker.list": "localhost:9092"})

    #Start Question 1
    dStream.foreachRDD(p1)
    #End Question 1

    context.start()
    context.awaitTermination()

def p1(time,rdd):

    rdd=rdd.map(lambda x: json.loads(x[1]))
    records=rdd.collect()

    records = [element for element in records if "delete" not in element] #remove delete tweets
    records = [element["entities"]["hashtags"] for element in records if "entities" in element] #select only hashtags part
    records = [x for x in records if x] #remove empty hashtags
    records = [element[0]["text"] for element in records]
    if not records:
        print("Lista Vacia")
    else:
        rdd = sc.parallelize(records)

        spark = getSparkSessionInstance(rdd.context.getConf())
        # Convert RDD[String] to RDD[Row] to DataFrame
        hashtagsDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(hashtag=x)))
        hashtagsDataFrame.createOrReplaceTempView("hashtags")
        hashtagsDataFrame = spark.sql("select hashtag, count(*) as total, current_timestamp() as timestamp from hashtags group by hashtag order by total desc limit 3")
        hashtagsDataFrame.write.mode("append").saveAsTable("hashtag1")

          
        ds=spark.sql("select hashtag, sum(total) as suma from hashtag1  group by hashtag order by suma desc limit 5")
        ds.show()

    print(time)

   
     

if __name__ == "__main__":
    print("Stating to read tweets")
    credentials = read_credentials()
    oauth = OAuth(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'], credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    twitter_stream = TwitterStream(auth=oauth)
    sc = SparkContext(appName="Project 2")
    checkpointDirectory = "/checkpoint"
    consumer()

