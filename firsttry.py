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


#####################################################################################
print()
print("====== RAW LIST=======")
print()

d= [1 , 2 , 3, 4, 5]

print(d)

print()
print("====== rdd LIST=======")
print()

rdd = sc.parallelize( d )
print(rdd.collect())


print()
print("====== add LIST=======")
print()


addlis = rdd.map( lambda x  : x + 10)
print(addlis.collect())


print()
print("====== mul LIST=======")
print()

mullis = rdd.map(lambda   x   :   x  *  10)
print(mullis.collect())


print()
print("====== Filter LIST=======")
print()

fillis = rdd.filter( lambda   x   :   x > 2 )
print(fillis.collect())


######################################
print()
print("===============String List==============")
strlis =["Monica" , "Monu", "Mon"]
rddstring=sc.parallelize(strlis)
print(rddstring.collect())