import os
import urllib.request

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
}

for url, path in urls_and_paths.items():
    urllib.request.urlretrieve(url, path)


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
##########Revision.
print()
print("======= FILE 1 =======")
print()

file1 = sc.textFile("file1.txt",1)

gymdata = file1.filter(lambda x :  'Gymnastics' in x)

mapsplit = gymdata.map(lambda x : x.split(","))

from collections import namedtuple

schema = namedtuple("schema",["txnno","txndate","custno","amount","category","product","city","state","spendby"])

schemardd = mapsplit.map(lambda x : schema(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]))

prodfilter = schemardd.filter(lambda x : 'Gymnastics' in x.product)

schemadf = prodfilter.toDF()

schemadf.show(5)

print()
print("=======csv df =======")
print()



csvdf = spark.read.format("csv").option("header","true").load("file3.txt")

csvdf.show(5)


print()
print("=======JSON df =======")
print()

jsondf = spark.read.format("json").load("file4.json")

jsondf.show(5)


print()
print("=======parquet df =======")
print()


parquetdf = spark.read.load("file5.parquet")

parquetdf.show(5)

collist= ["txnno", "txndate", "custno", "amount", "category", "product", "city", "state", "spendby"]


schemadf1= schemadf.select(*collist)
csvdf1 = csvdf.select(*collist)
jsondf1 = jsondf.select(*collist)
parquetdf1=parquetdf.select(*collist)

schemadf1.show(5)
csvdf1.show(5)
jsondf1.show(5)
parquetdf1.show(5)




uniondf = schemadf1.union(csvdf1).union(jsondf1).union(parquetdf1)

print()
print("=======uniondf =======")
print()

uniondf.show(5)

from pyspark.sql.functions import  *

procdf = ( uniondf.withColumn("txndate",expr("split(txndate,'-')[2]"))
           .withColumnRenamed("txndate","year")
           .withColumn("status",expr("case when spendby='cash' then 1 else 0 end"))
           .filter("txnno > 50000")
           )


procdf.show(10)

print()
print("=======agg df =======")
print()

from pyspark.sql.types import *

aggdf = procdf.groupby("category").agg(sum("amount").alias("total"))

aggdf.show()