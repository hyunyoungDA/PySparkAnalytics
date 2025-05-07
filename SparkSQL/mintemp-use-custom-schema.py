import os
os.environ['PYSPARK_PYTHON']="C:/Users/ohy04/anaconda3/envs/torch20210854/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "C:/Users/ohy04/anaconda3/envs/torch20210854/python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import functions as func

spark = SparkSession.builder.master('local')\
                    .appName('mintemp-dataframe')\
                    .getOrCreate()  

# 명시적으로 스키마 정의                     
schema = StructType([
    StructField('stationID', StringType(), True),
    StructField('date', IntegerType(), True),
    StructField('measure_type', StringType(), True),
    StructField('temperature', FloatType(), True)
])

# custom schema 사용하면서 데이터 load
df = spark.read.schema(schema).csv('file:///SparkCourse/SparkSQL/1800.csv')

minTemps = df.filter(df.measure_type == 'TMIN')

stationTemps = df.select('stationID','temperature')

# 1. select으로 각 stationID 별 temperature의 최솟값을 반환 
# 여기서 Spark는 자동으로 컬럼 이름을 min(temperature)로 변경 -> 보존됨 
minTempsByStation = stationTemps.groupby('stationID').min('temperature')
minTempsByStation.show()

# min(temperature)에 연산을 추가한 후 결과를 temperature라는 새로운 컬럼에 저장
# 그 후 새로운 객체에 select를 해서 stationID와 새로 생성한 temperature만 선택 
minTempsByStationF = minTempsByStation.withColumn('temperature',
                                                  func.round(func.col('min(temperature)')*0.1*(9.0 / 5.0) + 32.0, 2))\
                                                  .select('stationID', 'temperature').sort('temperature')
                                                  
results = minTempsByStationF.collect()

for result in results:
    print(f"{result[0]}\t\t{result[1]:.2f}")

