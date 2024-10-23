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
###### DATA 1

data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()



data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
df1.show()



data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]



cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()


# Register DataFrames as temporary views
df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")

##SQL operations on Spark DataFrames using SQL queries ##

print()
print("SQL operations on Spark DataFrames using SQL queries")
print()

print("===== 1. Basic SELECT Queries=====")
spark.sql("SELECT * FROM df").show()
print()
print("------Select 2 columns	")
print()
spark.sql("SELECT id, tdate FROM df ORDER BY id").show()
spark.sql("SELECT amount, product FROM df ORDER BY tdate").show()

print("===== 2. Filtering with WHERE Clause=====")
print("----- WHERE category='Exercise'	")
spark.sql("SELECT id, tdate, category FROM df WHERE category='Exercise'").show()
print("----- WHERE category='Exercise' ORDER BY id	")
spark.sql("SELECT id, tdate, category FROM df WHERE category='Exercise' ORDER BY id").show()
print("----- WHERE category='Exercise' AND spendby='cash'	")
spark.sql("SELECT id, tdate, category, spendby FROM df WHERE category='Exercise' AND spendby='cash'"). show()
print("----- WHERE category='Exercise' AND spendby='credit'	")
spark.sql("SELECT id, tdate, category, spendby FROM df WHERE category='Exercise' AND spendby='credit'"). show()


print("===== 3. IN and LIKE Clauses=====")
print("----------1n Operator	")
spark.sql("SELECT * FROM df WHERE category IN ('Exercise', 'Gymnastics')").show()
print("-------Product column Contains Gymnastics ('Like' Filter)	")
spark.sql("SELECT * FROM df WHERE product LIKE '%Gymnastics%'").show()

print("===== 4. Not Operator/ Filter=====")
spark.sql("SELECT * FROM df WHERE category != 'Exercise'").show()
print("----------For Multi value	")
spark.sql("SELECT * FROM df WHERE category not in ('Exercise','Gymnastics')").show()

print("===== 5. NULL Checks=====")
print("------- Where product is NULL	")
spark.sql("SELECT * FROM df WHERE product IS NULL").show()
print("------- Where product is Not NULL	")
spark.sql("SELECT * FROM df WHERE product IS NOT NULL").show()

print("===== 6. Aggregate Functions=====")
print("--- Max of id	")
spark.sql("SELECT MAX(id) FROM df").show()
print("--- Max Function of id as id max	")
spark.sql("SELECT MAX(id) as idmax FROM df").show()
rint("--- Min Function of id	")
spark.sql("SELECT MIN(id) FROM df").show()
print("--- Min Function of id as id min	")
spark.sql("SELECT MIN(id) as idmin FROM df").show()
print("--- No of row count")
spark.sql("SELECT COUNT(*) FROM df").show()

print("===== 19. Window Functions=====")
print("---- Window Row Number	")
spark.sql("SELECT category, amount, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS row_number FROM df").show()
print("---- Window Dense_Rank Number	")
spark.sql("SELECT category, amount, dense_rank() OVER (PARTITION BY category ORDER BY amount DESC) AS dense_rank FROM df").show()
print("---- Window Rank Number	")
spark.sql("SELECT category, amount, rank() OVER (PARTITION BY category ORDER BY amount DESC) AS rank_number FROM df").show()
print("---- Window Lead Function	")
spark.sql("SELECT category, amount, lead(amount) OVER (PARTITION BY category ORDER BY amount DESC) AS LEAD FROM df").show()
print("---- Window Lag Function	")
spark.sql("SELECT category, amount, lag(amount) OVER (PARTITION BY category ORDER BY amount DESC) AS LAG FROM df").show()


print("===== 20. JOIN Operations=====")
print("---- Inner Join (Take common Ids with Name & Product)	")
spark.sql("SELECT a.id, a.name, b.product FROM cust a JOIN prod b ON a.id=b.id").show()
print("---- LEFT Join (Take complete left table and its product from right table)	")
spark.sql("SELECT a.id, a.name, b.product FROM cust a LEFT JOIN prod b ON a.id=b.id").show()
print("---- RIGHT Join (Take complete right table and its name from left table)	")
spark.sql("SELECT a.id, a.name, b.product FROM cust a RIGHT JOIN prod b ON a.id=b.id").show()
print("---- FULL Join (All the ids from left table,AII the ids from right table with name & product)	")
spark.sql("SELECT a.id, a.name, b.product FROM cust a FULL JOIN prod b ON a.id=b.id").show()

print("===== 21. Anti Join and Semi Join =====")
print("---- LEFT ANTI JOIN (Take ids from left which don't have in right table)	")
spark.sql("SELECT a.id, a.name FROM cust a LEFT ANTI JOIN prod b ON a.id=b.id").show()
print("---- LEFT SEMI JOIN	")
spark.sql("SELECT a.id, a.name FROM cust a LEFT SEMI JOIN prod b ON a.id=b.id").show()

print("===== 22. HAVING Clause=====")
spark.sql("SELECT category, COUNT(category) AS Cnt FROM df GROUP BY category HAVING COUNT( category) > 1").show()

print("===== 23. Nested Queries=====")
spark.sql("""SELECT SUM(amount) AS total, con_date FROM (
SELECT id, tdate, FROM_UNIXTIME(UNIX_TIMESTAMP(tdate, 'MM-dd-yyyy'), 'yyyy-MM-dd') AS con_date
, amount, category, product, spendby FROM df
) GROUP BY con_date""").show()

# Stop the Spark context sc.stop()




