from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import re
import math

def tokenize(text):
    """Tokenize text using the same function as in the indexing process"""
    return re.findall(r'\w+', text.lower())

def calculate_bm25(tf, doc_len, avg_doc_len, idf, k1=1.0, b=0.75):
    """Calculate BM25 score for a single term in a document"""
    return idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_len / avg_doc_len)))

def calculate_doc_score(joined_entry, avg_doc_len, term_stats):
    """Calculate BM25 score for a document-term pair"""
    doc_id = joined_entry[0]
    ((term, tf), (doc_len, title)) = joined_entry[1]
    
    score = calculate_bm25(
        tf,
        doc_len,
        avg_doc_len,
        term_stats[term][1]  # idf
    )
    
    return (doc_id, (score, title))

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
    
    inverted_index_rdd = df_inverted_index.rdd.map(
        lambda row: ((row.term, row.doc_id), row.tf)
    )
    
    doc_stats_rdd = df_doc_stats.rdd.map(
        lambda row: (row.doc_id, (row.doc_length, row.title))
    )
    
    term_stats_rdd = df_term_stats.rdd.map(
        lambda row: (row.term, (row.doc_count, row.idf))
    )
    
    filtered_term_stats = term_stats_rdd.filter(
        lambda entry: entry[0] in query_terms
    ).collectAsMap()
    
    query_terms_in_index = set(filtered_term_stats.keys())
    
    if not query_terms_in_index:
        print("None of the query terms were found in the index.")
        return
    
    total_docs = doc_stats_rdd.count()
    total_length = doc_stats_rdd.map(lambda x: x[1][0]).sum()
    avg_doc_len = total_length / total_docs if total_docs > 0 else 0
    
    # Broadcast the average document length and filtered term stats
    avg_doc_len_broadcast = sc.broadcast(avg_doc_len)
    filtered_term_stats_broadcast = sc.broadcast(filtered_term_stats)
    
    # Filter to relevant terms
    relevant_terms_rdd = inverted_index_rdd.filter(
        lambda entry: entry[0][0] in query_terms_in_index
    )
    
    # Join with document stats and calculate scores
    joined_rdd = relevant_terms_rdd.map(
        lambda entry: (entry[0][1], (entry[0][0], entry[1]))
    ).join(doc_stats_rdd)
    
    scored_docs = joined_rdd.map(
        lambda entry: calculate_doc_score(entry, avg_doc_len_broadcast.value, filtered_term_stats_broadcast.value)
    )
    
    # Complete the scoring pipeline
    doc_scores = scored_docs.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1])
    ).map(
        lambda entry: (entry[0], entry[1][0], entry[1][1])
    ).sortBy(
        lambda entry: entry[1], ascending=False
    )

    # Get top 10 documents
    top_docs = doc_scores.take(10)
    
    print("\nTop 10 results:")
    for i, (doc_id, score, title) in enumerate(top_docs, 1):
        print(f"{i}. Document: {doc_id}")
        print(f"   Title: {title}")
        print(f"   Score: {score:.4f}")

if __name__ == "__main__":
    main()