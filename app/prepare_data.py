import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

df = spark.read.parquet("/a.parquet")

def normalize_text(text):
    if not text:
        return text
    return re.sub(r'\s+', ' ', text).strip()

# Register the UDF
normalize_udf = udf(normalize_text, StringType())

# Apply normalization to title column
df = df.withColumn("title", normalize_udf("title"))

n = 1000
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)

rows = df.collect()

for row in rows:
    # Replace non-ASCII characters with ASCII approximations
    title = row['title'].encode('ascii', 'ignore').decode('ascii')
    # Replace problematic characters with underscores
    title = re.sub(r'[^a-zA-Z0-9_\-.]', '_', title)
    filename = "data/" + str(row['id']) + "_" + title.replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])

print(f"Successfully created {len(rows)} documents in data/ directory")

df.write \
    .option("sep", "\t") \
    .mode("overwrite") \
    .csv("/index/data")
