from __future__ import division

import os
os.environ['SPARK_HOME'] = "/usr/lib/python2.7/site-packages/pyspark"

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local") \
    .appName("Linear Regression Model") \
    .config("spark.executor.memory", "1gb") \
    .getOrCreate()

sc = spark.sparkContext

df = spark.read.format("CSV").option("header", "true").load("./titanic.csv")
df = df.withColumn("Survived", df["Survived"].cast(IntegerType())) \
    .withColumn("Pclass", df["Pclass"].cast(IntegerType())) \
    .withColumn("Name", df["Name"].cast(StringType())) \
    .withColumn("Sex", df["Sex"].cast(StringType())) \
    .withColumn("Age", df["Age"].cast(IntegerType())) \
    .withColumn("Siblings/Spouses Aboard", df["Siblings/Spouses Aboard"].cast(IntegerType())) \
    .withColumn("Parents/Children Aboard", df["Parents/Children Aboard"].cast(IntegerType())) \
    .withColumn("Fare", df["Fare"].cast(FloatType()))

df.show()

### Questions ###

# Conditional Probability for a person to survive, given sex and class
## We need 3 columns, then we will group by sex and class and show the average
df_survival = df.select("Survived", "Pclass", "Sex")
df_survival = df_survival.groupBy("Sex", "Pclass").avg("Survived")
print('Assignment 3A')
print('Conditional Probability for a person to survive, given sex and class:')
df_survival.show()

# Probability of a child thats 10 years or younger, in 3rd class, to survive
## We need 3 columns like the previous, but now with age instead of sex
df_kids = df.select("Survived", "Pclass", "Age")
## We want to know the chance of children from 3rd class & ages 10 and below
df_kids = df_kids.filter( (df_kids.Pclass == 3) & (df_kids.Age <= 10))
total_number_of_kids = df_kids.count()
## Now we counted the total of 3rd class kids of 10 and younger, we want to filter out those who survive
df_kids = df_kids.filter(df_kids.Survived == True)
number_of_kids_who_survived = df_kids.count()
## We know how many kids there were, and how many survived, so lets calculate:
chance_to_survive = number_of_kids_who_survived / total_number_of_kids
chance_to_survive = chance_to_survive * 100
print('Assignment 3B')
print('Probability of a child thats 10 years or younger, in 3rd class, to survive:')
print(chance_to_survive, "%")
print

# Expected avarage fare, based on class
## We need 2 columns, we will group by class and show the average of fare
df_fare = df.select("Pclass", "Fare")
df_fare = df_fare.groupBy("Pclass").avg("Fare")
print('Assignment 3C')
print('Expected avarage fare, based on class:')
df_fare.show()

spark.stop()
