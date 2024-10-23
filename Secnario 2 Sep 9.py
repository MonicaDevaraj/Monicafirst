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

data = [
    ("A", "AA"),
    ("B", "BB"),
    ("C", "CC"),
    ("AA", "AAA"),
    ("BB", "BBB"),
    ("CC", "CCC")
]

df = spark.createDataFrame(data, ["child", "parent"],1)

df.show()

df.selectExpr("child","parent","concat(child, parent) as GrandParent").filter("length(GrandParent)==3").show()



###############################################################
# Creating a Spark session
spark = SparkSession.builder.appName("JoinExample").getOrCreate()

# Data Preparation
data = [(1,), (2,), (3,), (1,), (2,), (1,)]
df = spark.createDataFrame(data, ["id"])

# Showing the data
df.show()

# Creating two DataFrames (both are the same in this case)
df1 = df
df2 = df

# 1. Performing Inner Join
inner_join_df = df1.join(df2, df1["id"] == df2["id"], "inner")
print("Inner Join:")
inner_join_df.show()

# 2. Performing Full Outer Join
full_join_df = df1.join(df2, df1["id"] == df2["id"], "outer")
print("Full Outer Join:")
full_join_df.show()

###############################################################


from pyspark.sql.functions import  *


rdd1 = spark.sparkContext.parallelize([
    (1, 'raj'),
    (2, 'ravi'),
    (3, 'sai'),
    (5, 'rani')
],1)

rdd2 = spark.sparkContext.parallelize([
    (1, 'mouse'),
    (3, 'mobile'),
    (7, 'laptop')
],1)


df1 = rdd1.toDF(['id', 'name']).coalesce(1)
df2 = rdd2.toDF(['id', 'product']).coalesce(1)

# Show the DataFrames
df1.show()
df2.show()


listvalue= df2.select("id").rdd.flatMap(lambda x : x).collect()

print(listvalue)


filterdf = df1.filter( ~ df1['id'].isin(listvalue))

print()
print("====LIST FILTERING===")
print()

filterdf.show()




################################3


rdd1 = spark.sparkContext.parallelize([
    ("sai", 10),
    ("zeyo", 20),
    ("sai", 15),
    ("zeyo", 10 ),
    ("sai", 10 ),
],1)

df = rdd1.toDF(['name', 'amt']).coalesce(1)

df.show()

from pyspark.sql.functions import  *


aggdf = df.groupby("name").agg(

    sum("amt").alias("total"),
    count("name").alias("cnt"),
    collect_list("amt").alias("col_collect_list"),
    collect_set("amt").alias("col_collect_set")

)

aggdf.show()



