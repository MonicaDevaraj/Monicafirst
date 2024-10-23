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
####################################
print()
print("===== RAW LIST======")
rawlist = [  "State->TN~City->Chennai"    ,    "State->Kerala~City->Trivandrum"]
print(rawlist)

print()
print("===== RDD LIST======")

rddlist = sc.parallelize(rawlist)
print(rddlist.collect())

print()
print("======FLAT RDD LIST===============")

flat= rddlist.flatMap(lambda x : x.split("~"))
print(flat.collect())

print()
print("===============STATE RDD LIST============================")
state =flat.filter(lambda x : 'tate' in x)
print(state.collect())

print()
print("===============STATE REPLACE LIST============================")

staterep =state.map(lambda x : x.replace("State->",""))
print(staterep.collect())

print()
print("===============CITY RDD LIST============================")
city = flat.filter(lambda x : 'City' in x)
print(city.collect())

print()
print("===============CITY REPLACE LIST============================")
cityrep = city.map(lambda x : x.replace("City->",""))
print(cityrep.collect())

#######################################################################################################################
print("===== RAW DATA=====")
print()
data = sc.textFile("dt.txt")
data.foreach(print)
## SPLIT DATA
print()
print()
mapsplit = data.map(lambda x : x.split(","))
#### DEFINE SCHEMA
from collections import namedtuple

schema = namedtuple("schema",["txnno","txndate","amount","category","product","spendby"])

#### IMPOSE SCHEMA
schemardd = mapsplit.map(lambda x : schema(x[0],x[1],x[2],x[3],x[4],x[5]))

#### fILTER SPECIFIC COLUMN
print()
print("====== PRODUCT CONTAINS GYMNASTICS====")

prodfilter = schemardd.filter(lambda x : 'Gymnastics' in x.product)
prodfilter.foreach(print)
## Dataframe conversion

df = prodfilter.toDF()
df.show()

############################################
csvdf = spark.read.format("csv").option("header","true").load("prod.csv")

csvdf.show()

csvdf.createOrReplaceTempView("liya")

procdf = spark.sql("select * from liya where id > 1")

procdf.show()

jsondf = spark.read.format("json").load("file4.json")

jsondf.show()


parquetdf = spark.read.load("file5.parquet")


parquetdf.show()
################################
csvdf = spark.read.format("csv").load("dt.txt").toDF("txnno","txndate","amount","category","product","spendby")

csvdf.show()

seldf = csvdf.select("txndate","product")

seldf.show()

dropdf = csvdf.drop("txndate","product")

dropdf.show()

print("==========CATEGORY = 'EXERCISE'============")

onefil = csvdf.filter("category='Exercise'")
onefil.show()

print("==========CATEGORY = 'EXERCISE' and spendby = 'cash'============")
mulfilteror = csvdf.filter("category='Exercie' and spendby = 'cash'")
mulfilteror.show()

print("==========CATEGORY = 'EXERCISE' or  spendby = 'cash'============")
mulfilter = csvdf.filter("category='Exercie' or spendby = 'cash'")
mulfilter.show()

print("==========CATEGORY = 'EXERCISE' , 'Gymnastics'============")
infil = csvdf.filter("category in ('Exercis' , 'Gymnastics') ")
infil.show()

print("==========product like gymnastics============")
print()
likefil = csvdf.filter("    product like  '%Gymnastics%'")
likefil.show()

print("==========Product is null============")
print()
nullfilter = csvdf.filter(" product is null ")
nullfilter.show()

print("==========Product is not null============")
print()
notnullfilter = csvdf.filter("product is not null")
notnullfilter.show()

print("==========Mulfunctions at a time============")
print()
onefil = csvdf.filter(" category='Exercise' ").drop("product").select("txnno","txndate")

onefil.show()