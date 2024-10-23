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
print("Logic: step2: get list of winning teams in a list")

print("Logic: step3: for each team, count the number of times team repeated")
data1 = [('A','D','D'), ('B','A','A'), ('A','D','A')]

print("Different ways to create dataframe")
print('command used: spark.createDataFrame(data1, ["Col1", "Col2", "Win"])')
df1 = spark.createDataFrame(data1, ["Col1", "Col2", "Win"])
df1.show()

print("get unique list of all teams that played matches")

df3 = df1.select('Col1')
df4 = df1.select('Col2')
df4.show()
df3.show()
df5 = df3.union(df4).distinct()
df5.show()


df6= df1.groupBy('Win').count()
df6.show()

df7 = df5.join(df6, df5.Col1==df6.Win, "left")
df7.show()
df7 = df7.drop('Win')
df7.show()