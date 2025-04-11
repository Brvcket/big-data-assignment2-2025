#!/bin/bash

INPUT=${1:-/index/data}
HADOOP_STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar"

hdfs dfs -rm -r -f /tmp/index/output1
hdfs dfs -rm -r -f /tmp/index/output2

chmod +x mapreduce/mapper1.py mapreduce/reducer1.py
chmod +x mapreduce/mapper2.py mapreduce/reducer2.py

echo "Running indexing stage 1 on input: $INPUT"

hadoop jar $HADOOP_STREAMING_JAR \
  -files mapreduce/mapper1.py,mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "$INPUT" \
  -output /tmp/index/output1

echo "Running indexing stage 2 (document frequency calculation)"

hadoop jar $HADOOP_STREAMING_JAR \
  -files mapreduce/mapper2.py,mapreduce/reducer2.py \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py" \
  -input "/tmp/index/output1" \
  -output /tmp/index/output2

echo "Writing results to Cassandra..."
python3 app.py