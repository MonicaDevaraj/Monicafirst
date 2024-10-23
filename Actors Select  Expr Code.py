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



flatten1 =  df.selectExpr(

    "explode(Actors) as Actors",
    "country",
    "version"


)


flatten1.show()

flatten1.printSchema()


flatten2=flatten1.selectExpr(

    "Actors.Birthdate",
    "Actors.BornAt",
    "Actors.age",
    "Actors.hasChildren",
    "Actors.hasGreyHair",
    "Actors.name",
    "Actors.photo",
    "Actors.picture.large",
    "Actors.picture.medium",
    "Actors.picture.thumbnail",
    "Actors.weight",
    "Actors.wife",
    "country",
    "version"


)

flatten2.show()

flatten2.printSchema()