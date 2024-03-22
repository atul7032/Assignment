# Databricks notebook source
# Import necessary libraries from PySpark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import explode, concat, col, trim, lit

# COMMAND ----------

# Declare schema for the json data
json_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("payment", StructType([
        StructField("payment_id", StringType(), True),
        StructField("payment_date", StringType(), True),
        StructField("amount", StringType(), True)
    ]), True),
    StructField("reviews", ArrayType(StructType([
        StructField("review_id", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("comment", StringType(), True)
    ])), True)
])

# COMMAND ----------

# Read csv files into a DataFrame 
cust = spark.read.csv('dbfs:/FileStore/tables/customers.csv', header=True)
payment = spark.read.csv('dbfs:/FileStore/tables/payments.csv', header=True)
review = spark.read.csv('dbfs:/FileStore/tables/reviews.csv', header=True)

# Read the JSON file into a DataFrame with enforced schema
final_table_df = spark.read.json("dbfs:/FileStore/tables/cust_final_table.json", schema=json_schema, multiLine=True)

# COMMAND ----------

# remove whitespaces from the input data
cust = cust.select(*(trim(col(c)).alias(c) for c in cust.columns))
review = review.select(*(trim(col(c)).alias(c) for c in review.columns))
payment = payment.select(*(trim(col(c)).alias(c) for c in payment.columns))

# COMMAND ----------

cust.show()

# COMMAND ----------

payment.show()

# COMMAND ----------

review.show()

# COMMAND ----------

final_table_df.show(truncate=0)

# COMMAND ----------

# join the input dataframes
input_joined_df = cust.alias("cus") \
    .join(review, col("cus.customer_id") == review.customer_id, "left") \
    .join(payment, col("cus.customer_id") == payment.custmer_id, "left") \
    .select(
    concat(col("cus.customer_id"),"payment_id","review_id").alias("input_primary_key"),
    col("cus.customer_id"),
    "name",
    "email",
    "payment_id",
    "payment_date",
    "amount",
    "review_id",
    "rating",
    "comment"    
        )
input_joined_df.show()

# COMMAND ----------

# Explode the array column 'reviews' into separate rows and create a new column 'new_col_reviews'
final_table_df = final_table_df.withColumn("new_col_reviews", explode("reviews"))

# Select specific columns from the DataFrame and create a new DataFrame
final_table_df = final_table_df.select(
    # Concatenate values from different columns to create a unique identifier for each row
    concat("customer_id","payment.payment_id","new_col_reviews.review_id").alias("primary_key"),
    "customer_id",
    "name",
    "email",
    "payment.payment_id",
    "payment.payment_date",
    "payment.amount",
    "new_col_reviews.review_id",
    "new_col_reviews.rating",
    "new_col_reviews.comment")
final_table_df.show(truncate=0)

# COMMAND ----------

# remove whitespaces from the final data
final_table_df = final_table_df.select(*(trim(col(c)).alias(c) for c in final_table_df.columns))

# COMMAND ----------

# Find inserted_rows
inserted_rows = input_joined_df.join(final_table_df, input_joined_df.input_primary_key == final_table_df.primary_key, "left_anti")
inserted_rows = inserted_rows.withColumn("operation",lit('I'))
inserted_rows.show()

# COMMAND ----------

# Find deleted rows
deleted = final_table_df.join(input_joined_df, final_table_df.primary_key == input_joined_df.input_primary_key, "left_anti")
deleted = deleted.withColumn("operation",lit('D'))
deleted.show()

# COMMAND ----------

# Find updated rows
updated_rows = input_joined_df.alias("input").join(final_table_df, col("input.input_primary_key") == final_table_df.primary_key, "inner") \
    .filter(
        (col("input.name") != final_table_df.name) | 
        (col("input.email") != final_table_df.email) | 
        (col("input.payment_id") != final_table_df.payment_id) |
        (col("input.payment_date") != final_table_df.payment_date) | 
        (col("input.amount") != final_table_df.amount) | 
        (col("input.rating") != final_table_df.rating) | 
        (col("input.comment") != final_table_df.comment)
    ).select(
        col("input.input_primary_key"),
        col("input.customer_id"),
        col("input.name"),
        col("input.email"),
        col("input.payment_id"),
        col("input.payment_date"),
        col("input.amount"),
        col("input.review_id"),
        col("input.rating"),
        col("input.comment")
        )
updated_rows = updated_rows.withColumn("operation",lit('U'))
updated_rows.display()

# COMMAND ----------

# Union operation to combine three DataFrames: inserted_rows, deleted, and updated_rows.
union_df = inserted_rows.unionAll(deleted).unionAll(updated_rows)

# COMMAND ----------

union_df.display()

# COMMAND ----------

union_df.printSchema()

# COMMAND ----------

# Convert the DataFrame union_df to JSON format and collect the JSON strings into a list.
# Each JSON string represents a row in the DataFrame.
json_data = union_df.toJSON().collect()
for line in json_data:
    print(line)

# COMMAND ----------


