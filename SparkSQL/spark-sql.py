from pyspark.sql import SparkSession
from pyspark.sql import Row

import os 

os.environ['PYSPARK_PYTHON'] = "C:/Users/user/anaconda3/envs/torchenv/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "C:/Users/user/anaconda3/envs/torchenv/python.exe"

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# 각각의 행을 Row객체로 반환 
def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))

# spark.sparkContext -> 현재 SparkSession의 sparkContext를 사용하고 있으므로 이건 RDD 객체임 
lines = spark.sparkContext.textFile("SparkSQL/data/fakefriends.csv")

# DataFrame은 단지 Row 객체의 집합이므로 행들로 구성된 RDD가 필요함 
# people RDD 생성 
people = lines.map(mapper)

# cache -> 동일한 DataFrame을 여러번 사용할 때 or 복잡한 필터링이나 조인, 그룹 연산이 포함된 경우에 사용 
# OOM(Out Of Memory) 위험 존재 
schemaPeople = spark.createDataFrame(people).cache()

# 일시적인 View 생성 
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

for teen in teenagers.collect():
  print(teen)

# SQL query를 직접 사용하지 않고도 가능
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
