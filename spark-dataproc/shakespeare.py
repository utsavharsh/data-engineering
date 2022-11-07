#!/usr/bin/env python

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('yarn').appName('shakespeare').config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest.jar').getOrCreate()

words = spark.read.format('bigquery').option('table', 'bigquery-public-data:samples.shakespeare').load()

cnt = words.count
print("Number of rows in shakespeare :")
print(cnt)