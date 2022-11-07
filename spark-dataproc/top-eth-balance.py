#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master('yarn').appName('shakespeare').config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest.jar').getOrCreate()

eth_traces = spark.read.format('bigquery').option('table', 'bigquery-public-data:crypto_ethereum.traces').load()
eth_transactions = spark.read.format('bigquery').option('table', 'bigquery-public-data:crypto_ethereum.transactions').load()
eth_blocks = spark.read.format('bigquery').option('table', 'bigquery-public-data:crypto_ethereum.blocks').load()

debits = eth_traces.where(col("to_address").isNotNull() & (col("status") == 1) & (~col("call_type").isin('delegatecall', 'callcode', 'staticcall') | col("call_type").isNull()) ).select(col("to_address").alias("address"), col("value"), col("block_timestamp"))
credits = eth_traces.where(col("from_address").isNotNull() & (col("status") == 1) & (~col("call_type").isin('delegatecall', 'callcode', 'staticcall') | col("call_type").isNull()) ).select(col("from_address").alias("address"), (-1 * col("value")).alias("value"), col("block_timestamp"))
transactions_fee_debits = eth_transactions.join(eth_blocks, col("number") == col("block_number")).groupBy(col("miner").alias("address"), col("block_timestamp")).agg(sum(col("receipt_gas_used").cast("double") * (col("receipt_effective_gas_price") - coalesce(col("receipt_effective_gas_price"),lit(0))).cast("double")).alias("value")).select(col("address"), col("value"), col("block_timestamp"))
transactions_fee_credits = eth_transactions.select(col("from_address").alias("address"), (-1 * col("receipt_gas_used").cast("double") - col("receipt_effective_gas_price").cast("double")).alias("value"), col("block_timestamp"))

double_entry_book_grouped_by_date = debits.union(credits).union(transactions_fee_debits).union(transactions_fee_credits)
double_entry_book_grouped_by_date.show
