#!/bin/bash

INPUT=${1:-/index/data}
HADOOP_STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar"

hdfs dfs -rm -r -f /tmp/index/output1

chmod +x mapreduce/mapper1.py mapreduce/reducer1.py
# chmod +x mapreduce/mapper2.py mapreduce/reducer2.py

echo "Running indexing on input: $INPUT"

hadoop jar $HADOOP_STREAMING_JAR \
  -files mapreduce/mapper1.py,mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "$INPUT" \
  -output /tmp/index/output1


echo "Writing results to Cassandra..."
python3 app.py
