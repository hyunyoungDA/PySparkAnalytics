from pyspark import SparkConf, SparkContext
import os
os.environ['PYSPARK_PYTHON'] = "C:/Users/user/anaconda3/envs/torchenv/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "C:/Users/user/anaconda3/envs/torchenv/python.exe"

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/RDD/data/Book")

## words RDD에 split된 형태로 저장 
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue() # (단어, 각 단어의 빈도수)로 반환하는 countByValue함수 

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore') # 유니코드를 아스키 코드로 인코딩 
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
