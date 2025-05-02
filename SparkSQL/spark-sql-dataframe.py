from pyspark.sql import SparkSession, Row

import os 

os.environ['PYSPARK_PYTHON'] = "C:/Users/user/anaconda3/envs/torchenv/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "C:/Users/user/anaconda3/envs/torchenv/python.exe"

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

## 파일에 column 명과 같은 header가 있는 경우에 option을 사용해서 True로 추가해주거나 False로 제거할 수 있음 
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/SparkSQL/data/fakefriends-header.csv")

# 현재 데이터프레임의 스키마 확인 가능 
print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

# 새로운 열을 만드는 방법 
print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

spark.stop()

