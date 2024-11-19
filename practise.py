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

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r'C:\Users\Lenovo\.jdks\corretto-1.8.0_432'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

# spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

print("======= Started =======")

##[25] Integer list Iterations
# lst = [1, 2, 3, 4, 5]

# rddlst = sc.parallelize(lst)
#
# addrdd = rddlst.map(lambda x: x+5)
# mulrdd = rddlst.map(lambda x: x*5)
# divrdd = rddlst.map(lambda x: x/5)
#
# print("Add 5: ", addrdd.collect())
# print("Multiply 5: ", mulrdd.collect())
# print("Divide 5: ", divrdd.collect())
#


# lst = [1, 2, 3, 4, 5]

# rddlst = sc.parallelize(lst)
# print("====RDD List====")
# print(rddlst.collect())
#
# filterLst = rddlst.filter(lambda x: x>2)
# print("====Filtered List====")
# print(filterLst.collect())



##String list Iterations

# str = ["zeyobron", "zeyo", "analytics"]
# print("Original: ",str)
#
# rddstr = sc.parallelize(str)
#
# addedstr = rddstr.map(lambda x: x+" analytics")
# print("Append Analytics: ", addedstr.collect())
#
# strrdd = rddstr.map(lambda x: x.replace('zeyo', 'tera'))
# print("Replaced original: ",strrdd.collect())
#
# filterRdd = rddstr.filter(lambda x: "zeyo" in x.lower())
# print('Filtered RDD: ',filterRdd.collect())


#[25] FlatMap

# lst = ["A~B", "C~D"]
#
# lstRdd = sc.parallelize(lst)
#
# flatRdd = lstRdd.flatMap(lambda x: x.split('~'))
# print(flatRdd.collect())


##[25] Task - 1

# liststr= [ "A~B,C" ,  "D~E,F" ]
#
# rddlst = sc.parallelize(liststr)
# print(rddlst.collect())
#
# seplst = rddlst.flatMap(lambda x: x.split(',')).flatMap(lambda x: x.split('~'))
# print(seplst.collect())




## [26] Scenerio - 1

data = [
    "State->TN,City->Chennai",
    "State->UP,City->Lucknow"
]


# rddlst = sc.parallelize(data)
# print(rddlst.collect())
#
# splitdata = rddlst.flatMap(lambda x: x.split(','))
# print(splitdata.collect())
#
# states = splitdata.filter(lambda x: 'State' in x).map(lambda x: x.replace('State->',''))
# print(states.collect())
#
# city = splitdata.filter(lambda x: 'City' in x).map(lambda x: x.replace('City->',''))
# print(city.collect())



# Scenerio - 2

# data = ["bigdata~spark~hadoop~hive"]
#
# output = [
#     "Tech ->BIGDATA TRAINER->SAI",
#     "Tech ->SPARK TRAINER->SAI",
#     "Tech ->HADOOP TRAINER->SAI",
#     "Tech ->HIVE TRAINER->SAI",
# ]
#
# ls = sc.parallelize(data)
#
# finalrdd = ls.flatMap(lambda x: x.split('~')).map(lambda x: "Tech ->"+x.upper()+" Trainer->SAI")
# finalrdd.foreach(print)
# # print(finalrdd.collect())



#foreach (= loop)
#read RDD from file - state.txt

# data = [
#     "State->TN,City->Chennai",
#     "State->UP,City->Lucknow"
# ]
#
# # ls = sc.parallelize(data)
# ls = sc.textFile('state.txt')
# print('=====Data====')
# ls.foreach(print)
# print()
#
# splitls = ls.flatMap(lambda x: x.split(','))
# print('=====After Split====')
# splitls.foreach(print)
# print()
#
# statels = splitls.filter(lambda x: 'State' in x)
# print('=====Filter State====')
# statels.foreach(print)
# print()
#
# statelss = statels.map(lambda x: x.replace('State->', ''))
# print('=====Replace State->====')
# statelss.foreach(print)



# read as RDD from file - 'usdata.csv'


data = sc.textFile('usdata.csv')
# print(data.count())

filterData = data.filter(lambda x: len(x) > 200).flatMap(lambda x: x.split(','))\
.map(lambda x: x.replace('-','')).map(lambda x: x+" ,zeyo")

filterData.foreach(print)

















