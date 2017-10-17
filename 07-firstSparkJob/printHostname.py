#!/usr/bin/python
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("returnHostname").getOrCreate()
sc = spark.sparkContext

def returnHostname(id):
   retstr = os.uname()[1]
   return retstr

numparallel = 4
jobarray = sc.parallelize(range(numparallel))

jobrdd = jobarray.map(lambda x: returnHostname(x))
print(jobrdd.collect())

