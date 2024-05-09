# Databricks notebook source
# MAGIC %md
# MAGIC ![test image](files/shivam_panicker/images/DLT.png)

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# COMMAND ----------

kafka_bootstrap_servers_plaintext = dbutils.secrets.get("oetrta", "kafka-bootstrap-servers-plaintext")
startingOffsets = "earliest"
payments_topic = "dev_westpac_payments_demo_kafka"
payments_events_topic = "dev_westpac_payments_events_demo_kafka"
postings_topic = "dev_westpac_postings_demo_kafka"

# COMMAND ----------

westpac_payments_schema = StructType([     
            StructField('PaymentAmount', StringType(), True),
            StructField('PaymentDate', StringType(), True),
            StructField('PaymentType', StringType(), True),
            StructField('PaymentStatus', StringType(), True),
            StructField('PaymentFrom', StringType(), True),
            StructField('PaymentTo', StringType(), True)
            ])

# COMMAND ----------

westpac_postings_schema = StructType([     
            StructField('ExternalReferenceID', StringType(), True),
            StructField('PaymentID', StringType(), True),
            StructField('TransactionAmount', StringType(), True),
            StructField('TransactionDate', StringType(), True)
            ])

# COMMAND ----------

westpac_payment_events_schema = StructType([     
            StructField('paymentEventID', StringType(), True),
            StructField('ExternalReferenceID', StringType(), True),
            StructField('EventState', StringType(), True),
            StructField('EventDate', StringType(), True),
            StructField('_DeleteScore', StringType(), True)
            ])

# COMMAND ----------

@dlt.table
def bronze_payments(
  spark_conf={"pipelines.trigger.interval" : "10 seconds"}
):

  kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
    .option("subscribe", payments_topic)
    .option("startingOffsets", startingOffsets )
    .option("minPartitions", 200)  
    .load()
    .select(
      col("key").cast("string").alias("ExternalReferenceID"),
      from_json(col("value").cast("string"), westpac_payments_schema).alias("json_value"),
      col("json_value.PaymentAmount").alias("PaymentAmount"),
      col("json_value.PaymentDate").alias("PaymentDate"),
      col("json_value.PaymentType").alias("PaymentType"),
      col("json_value.PaymentStatus").alias("PaymentStatus"),
      col("json_value.PaymentFrom").alias("PaymentFrom"),
      col("json_value.PaymentTo").alias("PaymentTo")   
      )
    .drop("json_value")
    )
  return kafka


# COMMAND ----------

@dlt.table
def bronze_payments_events(
  spark_conf={"pipelines.trigger.interval" : "10 seconds"}
):

  kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
    .option("subscribe", payments_events_topic)
    .option("startingOffsets", startingOffsets )
    .option("minPartitions", 200)  
    .load()
    .select(
      col("key").cast("string").alias("paymentEventID"),
      from_json(col("value").cast("string"), westpac_payment_events_schema).alias("json_value"),
      col("json_value.ExternalReferenceID").alias("ExternalReferenceID"),
      col("json_value.EventState").alias("EventState"),
      col("json_value.EventDate").alias("EventDate"),
      col("json_value._DeleteScore").alias("_DeleteScore"),
      )
    .where("(_DeleteScore < 90 and EventState != 'Settled' ) OR (_DeleteScore < 90 and EventState = 'Settled') ") # Remove 10% of the records (Uniform Distribution)
    .drop("json_value")
    )
  return kafka

# COMMAND ----------

@dlt.table
def bronze_postings_events(
  spark_conf={"pipelines.trigger.interval" : "10 seconds"}
):

  kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
    .option("subscribe", postings_topic)
    .option("startingOffsets", startingOffsets )
    .option("minPartitions", 200)  
    .load()
    .select(
      col("key").cast("string").alias("PostingID"),
      from_json(col("value").cast("string"), westpac_postings_schema).alias("json_value"),
      col("json_value.ExternalReferenceID").alias("ExternalReferenceID"),
      col("json_value.TransactionAmount").alias("TransactionAmount"),
      col("json_value.TransactionDate").alias("TransactionDate"),  
      )
    .drop("json_value")
    )
  return kafka

# COMMAND ----------


