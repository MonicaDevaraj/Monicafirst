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



