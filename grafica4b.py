from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import Row, SparkSession
import pandas
import matplotlib.pyplot as plt


def sparksessioninit(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def hashtag_visualization():
    spark = sparksessioninit(sc.getConf())
    tiempo, fecha = sys.argv[1:]
    interval_time = ("%s %s"%(fecha,tiempo))
    ds=spark.sql("select keyword, sum(total) as suma from keywordt1 \
    where timestamp between cast('{}' as timestamp) - INTERVAL 1 HOUR and cast('{}' as timestamp) \
    group by keyword order by suma desc limit 10".format(interval_time, interval_time))
    s=ds.toPandas()
    showgrafic(s)
   # ds.show()

#ds=spark.sql("select keyword, sum(total) as suma from keywordt1  group by keyword order by suma desc limit 5")

def showgrafic(s):
    #explode = (0, 0.1, 0, 0)
    labels=s['keyword']
    values=s['suma']
    total= sum(values)
    fig1, ax1 = plt.subplots()
    ax1.pie(values, labels=labels, autopct= lambda p:'{:.0f}'.format(p * total /100), shadow=True, startangle=90)
   # ax1.pie(values, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
    ax1.axis('equal') 
    plt.show()

if __name__ == "__main__":
    sc = SparkContext(appName="Project 2")
    hashtag_visualization()
    #showgrafic()

