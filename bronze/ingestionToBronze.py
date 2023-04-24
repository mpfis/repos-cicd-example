# Databricks notebook source
from pyspark.sql.functions import *
spark.range(1000).withColumn("id2", col("id")*2).write.saveAsTable("mpflakehouse.demo.testtable_bronze", mode="overwrite")
