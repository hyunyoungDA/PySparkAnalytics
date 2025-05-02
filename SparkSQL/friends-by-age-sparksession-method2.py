from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round

spark = SparkSession.builder\
                    .appName('FriendsByAge-DataFrame')\
                    .master('local')\
                    .getOrCreate()

# csv 파일을 DataFrme으로 읽어옴       
# spark.read.option(key, value) -> CSV를 읽어올때 세부 옵션 설정 ; spark.read.csv로 해도 됨  
# header false-> CSV 파일의 첫 줄을 컬럼명으로 쓸건지 말건지 설정하는 것         
df =  spark.read.option('header', 'true').option('inferschema','true')\
  .csv("SparkSQL/data/fakefriends-header.csv")
  
print(df.printSchema())

# 직접 column 명을 지정해줌 
# 사람이 알아볼 수 있게 컬럼 이름을 직접 지정하는 것이 toDF()
# df = df.toDF('ID','Name',"Age","NumFriends")

# Age로 그룹화한 후에 각 Age 그룹마다 NumFriends 컬럼의 평균 값을 계산해서 AvgFriends로 별칭해서 그 결과를 새로운 변수에 저장 
averageFriendsByAge = df.groupby('age').agg(avg('friends').alias('AvgFriends'))

averageFriendsByAge = averageFriendsByAge.select(
    "age", round("AvgFriends", 2).alias("AvgFriends")
)

# 'Age'를 기준으로 정렬을 해서 action show
# show() 호출 시 실제 실행이 이루어지며, 그때까지 지연 평가되었던 모든 연산이 분산 환경에서 병렬로 처리되면서 결과 출력 
averageFriendsByAge.orderBy('age').show()

# SparkSession을 종료 -> 리소스 절약 
spark.stop()