from pyspark import SparkConf, SparkContext
import os
os.environ['PYSPARK_PYTHON'] = "C:/Users/user/anaconda3/envs/torchenv/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "C:/Users/user/anaconda3/envs/torchenv/python.exe"

conf = SparkConf().setMaster('local').setAppName('max-temperatures')
sc = SparkContext(conf = conf)

def parseLine(line):
  fields = line.split(',')
  stationID = fields[0] 
  entryType = fields[2] # temp information
  temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
  
  return (stationID, entryType, temperature)
  
# textFile로 한 줄씩 읽음.
# (stationID, entryType, temperature)
lines = sc.textFile('file:///SparkCourse/RDD/data/1800.csv')
parseLines = lines.map(parseLine) # lambda function도 사용 가능

# entryType이 TMAX인 데이터만 남김 (최고기온 데이터만)
maxTemps = parseLines.filter(lambda x: 'TMAX' in x[1])

# (StationID, temperature), entryType은 필요 없으므로 제외 
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))

maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))

results = maxTemps.collect()

for result in results:
  print(result[0] + "\t{:.2f}F".format(result[1]))




