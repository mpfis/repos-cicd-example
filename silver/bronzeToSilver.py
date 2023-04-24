# Databricks notebook source
from pyspark.sql.functions import *
spark.read.table("mpflakehouse.demo.testtable_bronze").withColumn("id3", col("id")*col("id2")*3).write.saveAsTable("mpflakehouse.demo.testtable_silver", mode="overwrite")
