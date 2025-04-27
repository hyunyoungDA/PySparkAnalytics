import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import collections

# SparkContext(sc)를 만들 때 사용한 문법 -> RDD 생성에 사용 
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

# local[*]: cpu 코어 전부 사용 
# spark = SparkSession().builder\
#                       .appName('RatingsHistogram')\
#                       .master('local[*]')\
#                       .getOrCreate()

sc = SparkContext(conf = conf)

# RDD 생성, u.data의 모든 줄은 하나의 RDD가 됨.
## RDD: 다양한 데이터 셋을 추상화한 것 
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

## lines.map -> action을 했다고 해서 lines RDD가 변하는 것은 아님
## ratings = lines.map과 같이 새로운 RDD에 반환해줘야됨 
ratings = lines.map(lambda x: x.split()[2])

# 데이터 분할 -> (평점, 개수)
result = ratings.countByValue() 

# key를 평점으로 해서 순서대로 정렬 
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

