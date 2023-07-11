#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Importing required packages 
import pandas as pd
import pyspark
import findspark
from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession.builder.appName("stream_csv_data").getOrCreate()


# In[3]:


spark


# In[4]:


df = spark.read.option("header", True).option("inferschema", True).csv("hdfs://localhost:9000/stream_csv_data/vaccination_all_tweets.csv")


# In[5]:


df.printSchema()


# In[6]:


# df.show(2)
df.limit(2).toPandas()


# In[16]:


# Basic Transforamtion
# 1. Drop unnecassary columns
# user_description
# user_created
# user_followers
# user_friends
# user_favourites
# favorites

df_clean = df.drop("user_description", "user_created", "user_followers", "user_friends", "user_favourites", "favorites")
#df_clean.show(2)


# In[12]:


df_final = df_clean.withColumnRenamed("date", "user_date_time")
df_final.printSchema()


# In[15]:


# Pushing the data to HDFS in JSON Format
df_final.repartition(1).write.format("json").mode("overwrite").option("header", True).option("inferschema", True).save("hdfs://localhost:9000/stream_json_data")

