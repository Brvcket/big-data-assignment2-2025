#!/bin/bash

# Check if query is provided
if [ $# -eq 0 ]; then
    echo "Usage: ./search.sh \"your search query\""
    exit 1
fi

# The query is all arguments combined
QUERY="$*"

echo "Searching for: $QUERY"

# Set environment variables for Python
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Submit the PySpark job to YARN
spark-submit \
    --master yarn \
    --deploy-mode client \
    --name "BM25 Search" \
    --conf spark.executor.memory=2g \
    --conf spark.driver.memory=2g \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
    --conf spark.cassandra.connection.host=cassandra-server \
    query.py "$QUERY"