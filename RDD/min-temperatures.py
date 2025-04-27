from pyspark import SparkConf, SparkContext
import os

os.environ['PYSPARK_PYTHON'] = "C:/Users/user/anaconda3/envs/torchenv/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "C:/Users/user/anaconda3/envs/torchenv/python.exe"

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2] ## Temp infomation
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///SparkCourse/data/1800.csv")
parsedLines = lines.map(parseLine)

# TMIN의 entryType의 값만 filtering해서 minTemps에 저장 
# TMIN에 해당하지 않는 값은 버려서 RDD의 크기를 줄임 
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

# (stationId. temperature)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))

# reduceByKey -> 동일한 key를 가진 값을 모아서 처리하는 함수 -> 두 개의 값을 하나씩 비교해서 순차비교 
# 여기서 x가 key의 값이 되는거고, y는 해당 key에 대한 여러 개의 temperature 
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
