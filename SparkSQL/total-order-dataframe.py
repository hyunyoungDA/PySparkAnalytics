import os
os.environ['PYSPARK_PYTHON']="C:/Users/ohy04/anaconda3/envs/torch20210854/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "C:/Users/ohy04/anaconda3/envs/torch20210854/python.exe"

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType

spark = SparkSession.builder.master('local')\
                    .appName('total-order')\
                    .getOrCreate()  
                    
schema = StructType([
    StructField('CustomerID', StringType(), True),
    StructField('date', IntegerType(), True),
    StructField('orders', FloatType(), True),
])
                    
df = spark.read.schema(schema).csv('file:///SparkCourse/SparkSQL/customer-orders.csv')

ordersByCustomer = df.select('CustomerID', 'orders')

customerGroup = ordersByCustomer.groupBy('CustomerID')\
                                .agg(func.round(func.sum('orders'), 2).alias('total_spent'))\
                                .orderBy('total_spent')
                                

results = customerGroup.collect()

for result in results:
    print(f"{result[0]}\t\t{result[1]:.2f}")
    
spark.stop()


