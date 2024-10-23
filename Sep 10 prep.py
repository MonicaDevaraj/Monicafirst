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
###Tuesday scenerio

data1 = [(1, "Monica"),(2, "Devaraj"),(3, "Vijay")]

columns1 = ["ID", "Name"]
rdd1 = sc.parallelize(data1,1)
df1 = rdd1.toDF(columns1)
df1.show()

data2 = [(1 , 1000),(2, 500),(4, 2000)]
columns2 = ["ID", "Salary"]
rdd2 = sc.parallelize(data2,1)
df2 = rdd2.toDF(columns2)
df2.show()

joindf = df1.join(df2, ['id'],'left')
joindf.show()

output_df = (joindf.withColumn( "salary",expr("""case when salary is null then '0' ELSE salary end"""))
             .drop("df2.ID")
             .orderBy('ID')
             )
print()
print("======================Output_Dataframe========================")
output_df.show()