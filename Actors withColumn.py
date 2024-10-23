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
data= """
{
  "country" : "US",
  "version" : "0.6",
  "Actors": [
    {
      "name": "Tom Cruise",
      "age": 56,
      "BornAt": "Syracuse, NY",
      "Birthdate": "July 3, 1962",
      "photo": "https://jsonformatter.org/img/tom-cruise.jpg",
      "wife": null,
      "weight": 67.5,
      "hasChildren": true,
      "hasGreyHair": false,
      "picture": {
                    "large": "https://randomuser.me/api/portraits/men/73.jpg",
                    "medium": "https://randomuser.me/api/portraits/med/men/73.jpg",
                    "thumbnail": "https://randomuser.me/api/portraits/thumb/men/73.jpg"
                }
    },
    {
      "name": "Robert Downey Jr.",
      "age": 53,
      "BornAt": "New York City, NY",
      "Birthdate": "April 4, 1965",
      "photo": "https://jsonformatter.org/img/Robert-Downey-Jr.jpg",
      "wife": "Susan Downey",
      "weight": 77.1,
      "hasChildren": true,
      "hasGreyHair": false,
      "picture": {
                    "large": "https://randomuser.me/api/portraits/men/78.jpg",
                    "medium": "https://randomuser.me/api/portraits/med/men/78.jpg",
                    "thumbnail": "https://randomuser.me/api/portraits/thumb/men/78.jpg"
                }
    }
  ]
}
"""


df = spark.read.json(sc.parallelize([data],1))
df.show()
df.printSchema()

from pyspark.sql.functions import *

flatten1 = (

    df.withColumn(
        "Actors",
        expr("explode(Actors)")

    )
)

flatten1.show()
flatten1.printSchema()



flatten2 = (
    flatten1.withColumn("Birthdate",expr("Actors.Birthdate"))
    .withColumn("BornAt",expr("Actors.BornAt"))
    .withColumn("age",expr("Actors.age"))
    .withColumn("hasChildren",expr("Actors.hasChildren"))
    .withColumn("hasGreyHair",expr("Actors.hasGreyHair"))
    .withColumn("name",expr("Actors.name"))
    .withColumn("photo",expr("Actors.photo"))
    .withColumn("large",expr("Actors.picture.large"))
    .withColumn("medium",expr("Actors.picture.medium"))
    .withColumn("thumbnail",expr("Actors.picture.thumbnail"))
    .withColumn("weight",expr("Actors.weight"))
    .withColumn("wife",expr("Actors.wife"))

).drop("Actors")


flatten2.show()
flatten2.printSchema()