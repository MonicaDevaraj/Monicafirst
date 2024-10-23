# ======================================================================================
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys
import os
from collections import namedtuple
from pyspark.sql.functions import *
from pyspark.sql.types import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Users\44772\.jdks\corretto-1.8.0_422'
conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")



sc = SparkContext(conf=conf)


spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20,False)

####################################################################################################


import urllib.request; exec(urllib.request.urlopen("https://gist.githubusercontent.com/saiadityaus1/889aa99339c5d5bc67f96d7420c46923/raw").read().decode('utf-8'))
##########################################################################################################
data = [
    ("monicadevaraj@gmail.com","9876543210"),
    ("amyjackson@gmail.com", "984456778")
]

rdd = spark.sparkContext.parallelize(data,1)
df = rdd.toDF(["mail","mob"])
df.show()

def mask_email(email):
    length = len(email.split('@')[0])
    return email[0]+ '*'*(length-2) +email[length-1:]

def mask_mobile(mobile):
    return mobile[0]+ '*'*(len(mobile)-4)+mobile[-2:]

mask_email_udf = udf(mask_email)
mask_mob_udf = udf(mask_mobile)
df.withColumn("mask_mail", mask_email_udf(col("mail"))) \
    .withColumn("mask_mob",mask_mob_udf(col("mob"))) \
    .select("mask_mail","mask_mob").show()
