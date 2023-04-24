# Databricks notebook source
from pyspark.sql.functions import *
spark.read.table("mpflakehouse.demo.testtable_silver").withColumn("id4", col("id3")*col("id2")*3).write.saveAsTable("mpflakehouse.demo.testtable_gold", mode="overwrite")
