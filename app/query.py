from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import re
import math

def tokenize(text):
    """Tokenize text using the same function as in the indexing process"""
    return re.findall(r'\w+', text.lower())

def calculate_bm25(tf, doc_len, avg_doc_len, idf, k1=1.2, b=0.75):
    """Calculate BM25 score for a single term in a document"""
    return idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_len / avg_doc_len)))

def extract_title(doc_id):
    """Extract title from document ID if it follows the format 'id_title'"""
    try:
        parts = doc_id.split("_", 1)
        print(doc_id)
        print(parts)
        print('-------------------')
        return parts[1].replace("_", " ") if len(parts) > 1 else doc_id
    except:
        return doc_id

def main():
    # Initialize Spark
    conf = SparkConf().setAppName("BM25 Document Ranking")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    
    # Read query from command line arguments
    if len(sys.argv) < 2:
        print("Usage: spark-submit query.py 'your search query'")
        return
    
    query = " ".join(sys.argv[1:])
    print(f"Searching for: {query}")
    
    # Tokenize query
    query_terms = tokenize(query)
    
    if not query_terms:
        print("Query is empty. Please provide search terms.")
        return
    
    # Connect to Cassandra and load data
    print("Loading inverted index from Cassandra...")
    df_inverted_index = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="inverted_index", keyspace="bigdata") \
        .load()
    
    df_doc_stats = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="doc_stats", keyspace="bigdata") \
        .load()
    
    df_term_stats = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="term_stats", keyspace="bigdata") \
        .load()
    
    # Convert to RDDs
    inverted_index_rdd = df_inverted_index.rdd.map(lambda row: ((row.term, row.doc_id), row.tf))
    doc_stats_rdd = df_doc_stats.rdd.map(lambda row: (row.doc_id, row.doc_length))
    term_stats_rdd = df_term_stats.rdd.map(lambda row: (row.term, (row.doc_count, row.idf)))
    
    # Filter to only include query terms
    filtered_term_stats = term_stats_rdd.filter(lambda x: x[0] in query_terms).collectAsMap()
    query_terms_in_index = set(filtered_term_stats.keys())
    
    if not query_terms_in_index:
        print("None of the query terms were found in the index.")
        return
    
    # Calculate average document length
    total_docs = doc_stats_rdd.count()
    total_length = doc_stats_rdd.map(lambda x: x[1]).sum()
    avg_doc_len = total_length / total_docs if total_docs > 0 else 0
    
    # Broadcast the average document length and filtered term stats
    avg_doc_len_broadcast = sc.broadcast(avg_doc_len)
    filtered_term_stats_broadcast = sc.broadcast(filtered_term_stats)
    
    # Filter relevant terms from the inverted index
    relevant_terms_rdd = inverted_index_rdd.filter(lambda x: x[0][0] in query_terms_in_index)
    
    # Join with document stats
    doc_scores = relevant_terms_rdd \
        .map(lambda x: (x[0][1], (x[0][0], x[1]))) \
        .join(doc_stats_rdd) \
        .map(lambda x: (
            x[0],  # doc_id
            calculate_bm25(
                x[1][0][1],  # tf
                x[1][1],  # doc_len
                avg_doc_len_broadcast.value,
                filtered_term_stats_broadcast.value[x[1][0][0]][1]  # idf
            )
        )) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False)
    
    # Get top 10 documents
    top_docs = doc_scores.take(10)
    
    print("\nTop 10 results:")
    for i, (doc_id, score) in enumerate(top_docs, 1):
        title = extract_title(doc_id)
        print(f"{i}. Document: {doc_id}")
        print(f"   Title: {title}")
        print(f"   Score: {score:.4f}")

if __name__ == "__main__":
    main()