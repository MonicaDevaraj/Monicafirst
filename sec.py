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
print()
print("====== RAW LIST=======")
print()

d= [1 , 2 , 3]

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


#############################
print()
print("====== raw string LIST=======")
print()


strlis = ["zeyobron" , "zeyo" , "analytics"]

rddstring = sc.parallelize( strlis )

print(rddstring.collect())

######################################################################################
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://github.com/saiadityaus1/test1/raw/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv"}.items()]


print()
print("========================RAW STRING LIST==============================")
print()

lisstr = [ "zeyobron" ,"zeyo","analytics" ]
print(lisstr)

print()
print("===========================RDD STRING LIST=============================")
print()

rddstr=sc.parallelize(lisstr)
print(rddstr.collect())

print()
print("============================Add STRING LIST===============================")
print()

addlis = rddstr.map( lambda x : x + " pvt")
print(addlis.collect())

print()
print("===============REPLACE STRING LIST================")
print()

reprdd = rddstr.map( lambda x : x.replace("zeyo","tera"))
print(reprdd.collect())


################################################################
print()
print("==========RAW STRING LIST============")
print()

lisstr = [ "A~B" , "C~D" ]
print(lisstr)

print()
print("==========RDD STRING LIST===============")
print()

rddstr=sc.parallelize(lisstr)
print(rddstr.collect())

print()
print("=========FlatMap STRING LIST===================")
print()

flat = rddstr.flatMap( lambda x : x.split("~"))
print(flat.collect())

##################################################33
print()
print("==========RAW STRING LIST============")
print()

rawstr = [ "zeyobron" , "zeyot" , "znalyticszeyo" ]
print(rawstr)

print()
print("==========Raw RDD Str============")
print()

rddstr = sc.parallelize(rawstr)
print(rddstr.collect())

print()
print("==========Remove Zeyo STR============")
print()

repstr = rddstr.map(lambda x : x.replace("zeyo",""))
print(repstr.collect())


