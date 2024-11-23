# import os
# import urllib.request
# import ssl
#
# data_dir = "data"
# os.makedirs(data_dir, exist_ok=True)
#
# data_dir1 = "hadoop/bin"
# os.makedirs(data_dir1, exist_ok=True)
#
# urls_and_paths = {
#     "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
#     "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir1, "winutils.exe"),
#     "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir1, "hadoop.dll")
# }
#
# # Create an unverified SSL context
# ssl_context = ssl._create_unverified_context()
#
# for url, path in urls_and_paths.items():
#     # Use the unverified context with urlopen
#     with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
#         data = response.read()
#         out_file.write(data)
# import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://github.com/saiadityaus1/test1/raw/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys,os

from pythonPractise import numbers

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r'C:\Users\Lenovo\.jdks\corretto-1.8.0_432'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

# spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

print("======= Started =======")

#[25] Integer list Iterations
lst = [1, 2, 3, 4, 5]

rddlst = sc.parallelize(lst)

addrdd = rddlst.map(lambda x: x+5)
mulrdd = rddlst.map(lambda x: x*5)
divrdd = rddlst.map(lambda x: x/5)

print("Add 5: ", addrdd.collect())
print("Multiply 5: ", mulrdd.collect())
print("Divide 5: ", divrdd.collect())



lst = [1, 2, 3, 4, 5]

rddlst = sc.parallelize(lst)
print("====RDD List====")
print(rddlst.collect())

filterLst = rddlst.filter(lambda x: x>2)
print("====Filtered List====")
print(filterLst.collect())



#String list Iterations

str = ["zeyobron", "zeyo", "analytics"]
print("Original: ",str)

rddstr = sc.parallelize(str)

addedstr = rddstr.map(lambda x: x+" analytics")
print("Append Analytics: ", addedstr.collect())

strrdd = rddstr.map(lambda x: x.replace('zeyo', 'tera'))
print("Replaced original: ",strrdd.collect())

filterRdd = rddstr.filter(lambda x: "zeyo" in x.lower())
print('Filtered RDD: ',filterRdd.collect())


# [25] FlatMap
#
lst = ["A~B", "C~D"]

lstRdd = sc.parallelize(lst)

flatRdd = lstRdd.flatMap(lambda x: x.split('~'))
print(flatRdd.collect())


##[25] Task - 1

liststr= [ "A~B,C" ,  "D~E,F" ]

rddlst = sc.parallelize(liststr)
print(rddlst.collect())

seplst = rddlst.flatMap(lambda x: x.split(',')).flatMap(lambda x: x.split('~'))
print(seplst.collect())




## [26] Scenerio - 1


data = [
    "State->TN,City->Chennai",
    "State->UP,City->Lucknow"
]


rddlst = sc.parallelize(data)
print(rddlst.collect())

splitdata = rddlst.flatMap(lambda x: x.split(','))
print(splitdata.collect())

states = splitdata.filter(lambda x: 'State' in x).map(lambda x: x.replace('State->',''))
print(states.collect())

city = splitdata.filter(lambda x: 'City' in x).map(lambda x: x.replace('City->',''))
print(city.collect())



Scenerio - 2

data = ["bigdata~spark~hadoop~hive"]

output = [
    "Tech ->BIGDATA TRAINER->SAI",
    "Tech ->SPARK TRAINER->SAI",
    "Tech ->HADOOP TRAINER->SAI",
    "Tech ->HIVE TRAINER->SAI",
]

ls = sc.parallelize(data)

finalrdd = ls.flatMap(lambda x: x.split('~')).map(lambda x: "Tech ->"+x.upper()+" Trainer->SAI")
finalrdd.foreach(print)
# print(finalrdd.collect())



foreach (= loop)
read RDD from file - state.txt

data = [
    "State->TN,City->Chennai",
    "State->UP,City->Lucknow"
]

# ls = sc.parallelize(data)
ls = sc.textFile('state.txt')
print('=====Data====')
ls.foreach(print)
print()

splitls = ls.flatMap(lambda x: x.split(','))
print('=====After Split====')
splitls.foreach(print)
print()

statels = splitls.filter(lambda x: 'State' in x)
print('=====Filter State====')
statels.foreach(print)
print()

statelss = statels.map(lambda x: x.replace('State->', ''))
print('=====Replace State->====')
statelss.foreach(print)



read as RDD from file - 'usdata.csv'


data = sc.textFile('usdata.csv')
# print(data.count())

filterData = data.filter(lambda x: len(x) > 200).flatMap(lambda x: x.split(','))\
.map(lambda x: x.replace('-','')).map(lambda x: x+" ,zeyo")

filterData.foreach(print)




[27]: RDD Col based processing - dt.txt

data = sc.textFile('dt.txt')
print('======File Data======')
data.foreach(print)


splitdata = data.map(lambda x: x.split(','))
print('=====Split Data=====')
splitdata.foreach(print)


from collections import namedtuple

columns = namedtuple('columns',['id','tno','amt','category','product','mode'])

coldata = splitdata.map(lambda x: columns(x[0],x[1],x[2],x[3],x[4],x[5]))
print('=====Data with columns Data=====')
coldata.foreach(print)


filterData = coldata.filter(lambda x: 'Gymnastics' in x.product)
print('=====Filterd rows=====')
# print(filterData.collect())
filterData.foreach(print)


print('====Print as Dataframe=========')
df = filterData.toDF()
df.show()


df.createOrReplaceTempView('table')
spark.sql("""
    SELECT id, tno from table;
""").show()


#[27] SQL Practise - Video

print("========= DATA PREPARATION======")

data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 300.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 100.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])



data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])



data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]


cust = spark.createDataFrame(data4, ["id", "name"])




data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])

df.show()
df1.show()
# cust.show()
# prod.show()

# Register DataFrames as temporary views
df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")


print("=========DONE GOOD TO GO BELOW======")


spark.sql("""
        select id,product from df;
""").show()


spark.sql("""
        select * from df where category='Exercise';
""").show()


spark.sql("""
    select id, tdate, category, spendby from df where category='Exercise' and spendby = 'cash';
""").show()

spark.sql("""
    SELECT * from df
    where category IN ('Exercise', 'Gymnastics');
""").show()


spark.sql("""
    SELECT * from df
    WHERE product LIKE '%Gymnastics%';
""").show()


spark.sql("""
    SELECT * from df
    WHERE category <> 'Exercise';
""").show()


spark.sql("""
    SELECt * from df
    where Category <> 'Exercise' and Category <> 'Gymnastics';
""").show()


spark.sql("""
    select * from df
    where product is null;
""").show()

spark.sql("""
    SELECT max(id) as maxId, min(id) as minId, count(1) as Count from df;
""").show()


spark.sql("""
    SELECT *,
    CASE
        WHEN spendby = 'cash' THEN 1
        WHEN spendby = 'credit' THEN 0 ELSE 'na'
    END AS status
    FROM df;
""").show()


spark.sql("""
    select *,
    concat(id,"-",category) as condata
    from df
""").show()


spark.sql("""
    select *,
    concat_ws('-', id,category,product) as condata
    from df
""").show(truncate=False)


spark.sql("""
    select lower(category) as smallcase,
    upper(category) as uppercase
     from df
""").show()


spark.sql("""
    select amount, ceil(amount), floor(amount), round(amount) from df
""").show()


#coalesce - replace null with value given
spark.sql("""
    select product,
    coalesce(product, 'NA') as nullReplace
    from df
""").show()


trim - leading and trailing spaces
spark.sql("""
    select trim(product) as trimmed from df
""").show()


#distinct
spark.sql("""
    SELECT
     DISTINCT(category,spendby) as multiDistinct
    from df
""").show(truncate=False)


#substring
spark.sql("""
    SELECT substring(product, 1, 4) as substring
    from df
""").show()


#split and get first element
spark.sql("""
    SELECT product, split(product,' ')[0] as splitted from df
""").show()


sum of each category - group by
sum of category, spendby and count
sum of category, spendby and count and max(amount) per category
spark.sql("""
    select  max(amount) as max_amt_category, category, spendby, SUM(amount) as categorySum, Count(1) as combinedNo
    from df
    group by category, spendby
    order by category desc
""").show()


Window functions
1. Row numbers
2. Rank
3. Dense Rank

spark.sql("""
    select category,amount,
    row_number() OVER (partition by category order by amount asc) as rn,
    rank() OVER (partition by category order by amount asc) as rnk,
    dense_rank() OVER (partition by category ORDER BY amount asc) as d_rnk
    from df
""").show()


# 4. Lead
# 5. Lag

spark.sql("""
    select category, amount,
    lead(amount) over (partition by category ORDER BY amount desc) as lead_amt,
    lag(amount) over (partition by category ORDER BY amount desc) as lag_amt
    from df
""").show()


# having clause
spark.sql("""
    select category, count(1) as cnt
    from df
    group by category
    having count(1)>1
""").show()
