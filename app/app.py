from cassandra.cluster import Cluster
import subprocess

def init_cassandra():
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS bigdata 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    session.set_keyspace('bigdata')
    session.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            term text,
            doc_id text,
            tf int,
            PRIMARY KEY (term, doc_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS doc_stats (
            doc_id text PRIMARY KEY,
            doc_length int
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS term_stats (
            term text PRIMARY KEY,
            doc_count int,
            idf double
        )
    """)
    return session

def read_hdfs_output(hdfs_path):
    result = subprocess.run(
        ["hdfs", "dfs", "-cat", f"{hdfs_path}/part-*"],
        stdout=subprocess.PIPE,
        encoding="utf-8",
        check=True
    )
    return result.stdout.splitlines()

def process_stage1_output(session, lines):
    for line in lines:
        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue
        key, count = parts
        count = int(count)

        if key.startswith("DOCLEN_"):
            doc_id = key.replace("DOCLEN_", "")
            session.execute(
                "INSERT INTO doc_stats (doc_id, doc_length) VALUES (%s, %s)",
                (doc_id, count)
            )
        else:
            try:
                term, doc_id = key.split("::")
                session.execute(
                    "INSERT INTO inverted_index (term, doc_id, tf) VALUES (%s, %s, %s)",
                    (term, doc_id, count)
                )
            except ValueError:
                continue

def process_stage2_output(session, lines):
    for line in lines:
        parts = line.strip().split("\t")
        if len(parts) != 3:
            continue
        
        term, doc_count, idf = parts
        session.execute(
            "INSERT INTO term_stats (term, doc_count, idf) VALUES (%s, %s, %s)",
            (term, int(doc_count), float(idf))
        )

def main():
    session = init_cassandra()
    
    print("Processing stage 1 output...")
    stage1_lines = read_hdfs_output("/tmp/index/output1")
    process_stage1_output(session, stage1_lines)
    
    print("Processing stage 2 output...")
    stage2_lines = read_hdfs_output("/tmp/index/output2")
    process_stage2_output(session, stage2_lines)
    
    print("Indexing complete!")

if __name__ == "__main__":
    main()
