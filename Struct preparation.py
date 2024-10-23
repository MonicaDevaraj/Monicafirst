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
    "id": 2,
    "trainer": "Monica",
    "zeyoaddress": {
            "permanentAddress": "Hyderabad",
            "temporaryAddress": "Chennai"
    }
}
"""
df = spark.read.json(sc.parallelize([data],1))
df.show()
df.printSchema()
flattendf = df.selectExpr(
    "id",
    "trainer",
    "zeyoaddress.permanentAddress",
    "zeyoaddress.temporaryAddress",
)

flattendf.show()
flattendf.printSchema()



data= """

{
    "id": 2,
    "trainer": "Sai",
    "zeyoaddress": {
        "user": {
            "permanentAddress": "Hyderabad",
            "temporaryAddress": "Chennai",
            "postAddress": {
                "doorno": 14,
                "street": "sriram nagar"
            }
        }
    }
}

"""
df = spark.read.json(sc.parallelize([data],1))
df.show()
df.printSchema()

flattendf = df.selectExpr(

    "id",
    "trainer",
    "zeyoaddress.user.permanentAddress",
    "zeyoaddress.user.postAddress.doorno",
    "zeyoaddress.user.postAddress.street",
    "zeyoaddress.user.temporaryAddress"

)

flattendf.show()

flattendf.printSchema()

#ARRAY WITH STRUCT

data= """

{
    "id": 2,
    "trainer": "Sai",
    "zeyoaddress": {
        "permanentAddress": "Hyderabad",
        "temporaryAddress": "Chennai"
    },
    "zeyoStudents": [
        "Ajay",
        "rani"
    ]
}
"""
df = spark.read.json(sc.parallelize([data],1))
df.show()
df.printSchema()

flattendf = df.selectExpr(
    "id",
    "trainer",
    "explode(zeyoStudents) as zeyoStudents",
    "zeyoaddress.permanentAddress",
    "zeyoaddress.temporaryAddress"
)

flattendf.show()
flattendf.printSchema()


#=================Struct Inside Array==================#

data= """


{
    "id": 2,
    "trainer": "Sai",
    "zeyoStudents": [
        {
            "user": {
                "name": "Ajay",
                "age": 21
            }
        },
        {
            "user": {
                "name": "Rani",
                "age": 24
            }
        }
    ]
}

"""
df = spark.read.json(sc.parallelize([data],1))
df.show()
df.printSchema()



flatten1 = df.selectExpr(

    "id",
    "trainer",
    "explode(zeyoStudents) as zeyoStudents"
)

flatten1.show()

flatten1.printSchema()




flatten2 = flatten1.selectExpr(

    "id",
    "trainer",
    "zeyoStudents.user.age",
    "zeyoStudents.user.name"



)

flatten2.show()

flatten2.printSchema()