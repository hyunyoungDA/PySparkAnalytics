from pyspark.sql import SparkSession, Row
import os 

os.environ['PYSPARK_PYTHON'] = "C:/Users/user/anaconda3/envs/torchenv/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "C:/Users/user/anaconda3/envs/torchenv/python.exe"

spark = SparkSession.builder\
                    .master('local')\
                    .appName('friend-by-age-sparksession')\
                    .getOrCreate()
## Method 1
## sparkContext로 RDD 객체를 생성한 후에 r각 행에 대해 Row 객체로 변환하고 DataFrame 생성                    
def mapper(line):
  fields = line.split(',')
  return Row(ID = int(fields[0]), name = str(fields[1].encode('utf-8')),\
              age = int(fields[2]), numFriends = int(fields[3]))
  
data1 = spark.sparkContext.textFile("SparkSQL/data/fakefriends-header.csv")

header = data1.first()
people = data1.filter(lambda line: line != header).map(mapper)

schemaPeople = spark.createDataFrame(people).cache()

schemaPeople.createOrReplaceTempView('people')

schemaPeople.groupBy('age').count().orderBy('count', ascending = False).show()

spark.stop()


# ## Method 2         
# header -> 컬럼 이름을 schema로 정의할 땐 header로 충분하지만 데이터 타입까지 정의하고 싶다면 inferschema 
# inferschema -> 컬럼의 데이터 타입을 자동으로 추론 
# data2 = spark.read.option('header', 'true').option('inferschema', 'true')\
#   .csv("file:///SparkCourse/SparkSQL/data/fakefriends-header.csv")
  
# data2.groupBy('age').count().orderBy('count').show()