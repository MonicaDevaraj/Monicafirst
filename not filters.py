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


data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", "", "cash")
]

# Create an RDD from the data
rdd = spark.sparkContext.parallelize(data)

# Convert RDD to DataFrame using toDF() and specify column names
columns = ["txnno", "txndate", "amount", "category", "product", "spendby"]
csvdf = rdd.toDF(columns)
print()
print("===== raw dataframe ===")
print()

csvdf.show()

print()
print("===== Category not equal to Exercise ===")
print()

notfilter = csvdf.filter(" category != 'Exercise' ")
notfilter.show()

print()
print("===== category=Exercise and spendby=cash ===")
print()

notfil1 = csvdf.filter(" category='Exercise' and spendby !='cash' ")
notfil1.show()
#################################333

data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
]

rdd = spark.sparkContext.parallelize(data)
columns = ["txnno", "txndate", "amount", "category", "product", "spendby"]
csvdf = rdd.toDF(columns)
print()
print("===== raw dataframe ===")
print()

csvdf.show()

print()
print("===== Proc Dataframe ===")
print()

procdf = (
    csvdf.selectExpr(

        "txnno",   # column select
        "split(txndate, '-') [2] as year",  # column select
        "amount+100  as amount",   # Expression
        "upper(category)      as category",   # Expression
        "lower(product) as product",   # Expression
        "spendby",
        """case
            when spendby='cash' then 0
            when spendby='paytm' then 2
            else 1
            end
            as status""",
        "concat(category, '~zeyo') as con"
    )
)
procdf.show()

##############################################33
data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
]

rdd = spark.sparkContext.parallelize(data)
columns = ["txnno", "txndate", "amount", "category", "product", "spendby"]
csvdf = rdd.toDF(columns)
print()
print("===== raw dataframe ===")
print()

csvdf.show()


print()
print("=====EXPRESSIONS=====")
print()


from pyspark.sql.functions import *


#.SelectExpr("case when spendby ='cash' then 0 else 1 end as status")

procdf = (

    csvdf.withColumn( "category" , expr("upper(category)"))
    .withColumn("amount", expr("amount+100"))
    .withColumn("txnno",expr("cast(txnno as int)"))
    .withColumn("txndate",expr("split(txndate,'-')[2]"))
    .withColumn("product",expr("lower(product)"))
    .withColumn("spendby",expr("spendby"))
    .withColumn("status",expr("case when spendby ='cash' then 0 else 1 end"))
    .withColumn("con",expr("concat(category,'~zeyo')")))
