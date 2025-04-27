import os

# Spark는 Java기반이므로 Python코드 실행 시 현재 사용하고 있는 Python이 뭔지 시스템에 알려줘야됨
# 가상 환경에서의 pyspark 환경변수 설정 
os.environ['PYSPARK_PYTHON']="C:/Users/user/anaconda3/envs/torchenv/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "C:/Users/user/anaconda3/envs/torchenv/python.exe"

# Spark 1.x, Spark 2.x
# 전통적인 RDD 구현 방법 
from pyspark import SparkConf, SparkContext

# # SparkConf, SparkContext를 통합한 방법 -> DataFrame으로 최적화 잘 되어있어서 효율적 
# from pyspark.sql import SparkSession

# spark = SparkSession.builder\
#                     .appName("FriendsByAge")\
#                     .master("local")\
#                     .getOrCreate()

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# sc = spark.sparkContext

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///SparkCourse/data/fakefriends.csv")
rdd = lines.map(parseLine)

# mappValues -> 만약 데이터 (33, 385)이라면, (33, (385 , 1))로 반환
# 33세의 평균 친구의 수를 구하기 위해서는 각 33세의 친구의 수와 33세가 등장한 수를 알아야 평균 값을 알 수 있으므로
# reduceByKey -> 그래서 각 33세의 평균 친구의 수 x[0] + y[0] 계산, 특정 나이의 등장 횟수 x[1] + y[1] 계산 
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# 각 나이의 친구의 평균 수를 구함 
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

## DAG로 Spark가 최적의 방법을 찾아서 작동 
results = averagesByAge.collect()
for result in results:
    print(result)
