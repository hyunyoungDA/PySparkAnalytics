import os
os.environ['PYSPARK_PYTHON']="C:/Users/ohy04/anaconda3/envs/torch20210854/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "C:/Users/ohy04/anaconda3/envs/torch20210854/python.exe"

import re
from pyspark.sql import SparkSession,Row

# math 와 같은 다양한 함수 라이브러리 
from pyspark.sql import functions as func

spark = SparkSession.builder.master('local').appName('word-count-dataframe')\
                    .getOrCreate()
                    
df = spark.read.text('file:///SparkCourse/SparkSQL/Book')

# DataFrame.value -> Spark에서 비정형 텍스트 파일을 불러오면 기본 컬럼명은 value임 
# Spark에서는 각 행(row)을 RDD의 원소처럼 병렬처리할 수 있도록 함 
# explode: 리스트를 행으로 펼쳐서 결국 한 단어마다 한 행으로 구성된다.
words = df.select(func.explode(func.split(df.value, '\\W+')).alias('word'))

# 결측치를 제외한 모든 단어 
words.filter(words.word != "")

# words 데이터 프레임의 word columns의 각 word들을 소문자로 변경하고 그 값을 word라는 열에 저장
# word column을 포함한 데이터프레임 lowercaseWords 새로 정의 
lowercaseWords = words.select(func.lower(words.word).alias('word'))

# 각 word별 빈도수 계산 
wordCounts = lowercaseWords.groupby('word').count()

# 빈도순으로 정렬 
wordCountsSorted = wordCounts.sort('count')

# wordCountsSorted의 수만큼, 즉 모든 단어를 출력 
wordCountsSorted.show(wordCountsSorted.count())


