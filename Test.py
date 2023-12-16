# Databricks notebook source
# MAGIC %md
# MAGIC Notebook used to test theory or concepts

# COMMAND ----------

# MAGIC %md
# MAGIC Wide transformations
# MAGIC - groupByKey()
# MAGIC - reduceByKey()
# MAGIC - join()
# MAGIC - distinct()

# COMMAND ----------

# Wide tranformaiton example - join()
# Create two DataFrames
df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df2 = spark.createDataFrame([(1, "New York"), (2, "London")], ["id", "city"])
 
# Perform a join operation
joined_df = df1.join(df2, "id")
 
# Show the result
joined_df.show()

# COMMAND ----------

joined_df.collect()
joined_df.take(2)

# COMMAND ----------

from pyspark.sql.functions import col

df_man = spark.createDataFrame([(0, "Alice Jenkins"), (1, "Bob Newhart")], ["storeid", "managerName"])

df_man.withColumn("managerFirstName", col("managerName").split(" ").getItem(0))
# .withColumn("managerLastName", col("managerName").split(" ").getItem(1))

display(df_man)

# COMMAND ----------

# MAGIC %md
# MAGIC - df = spark.read.load("examples/src/main/resources/people.json", format="json")
# MAGIC - df.select("name", "age").write.save("namesAndAges.parquet", format="parquet"
# MAGIC ------------------
# MAGIC - storesDf.write.partitionBy('division').parquet(filepath)
# MAGIC - spark.read.json(filePath, schema = schema)

# COMMAND ----------

from pyspark.sql.functions import split

storesDF = spark.createDataFrame([(0, "true", "value_medium"), (1, "true", "mainstream_small"), (2, "false", "premium_large")],  ["id", "open", "storeCategory"])
split_df = (storesDF
            .withColumn("storeValueCategory", 
                split("storeCategory","_")[0])
            .withColumn("storeSizeCategory",
                split("storeCategory","_")[1])
            .drop("storeCategory")
)
display(split_df)

# COMMAND ----------

# MAGIC %md
# MAGIC User Defined functions (UDF) sysntax
# MAGIC - spark.udf.register("ASSESS_PERFORMANCE", assessPerformance)

# COMMAND ----------

# Crossjoin
result = storesDF.crossJoin(employeesDF)

# inner join
- storesDF.join(employeesDF, "storeID", "inner")

# COMMAND ----------

storesDF.describe(all=True)


# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col
# Regex replace syntax
storesDF.withColumn("storeDescription", regexp_replace(col("storeDescription"), "^Description: ", ""))

# COMMAND ----------

from pyspark.sql.functions import col

storesDF = spark.createDataFrame([(0, 43161), (1, 51200), (2, None), (3, 78367)], (["id", "sqft"]))
display(storesDF.na.fill(30000, "sqft"))
display(storesDF.top_n(3))



# COMMAND ----------

from pyspark.sql.functions import col, mean
display(storesDF.agg(mean("sqft")))

# COMMAND ----------

display(df_man.orderBy(col("managerName").desc()))
