from cassandra.cluster import Cluster
import subprocess
import os

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
            doc_length int,
            title text
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

def read_document_titles(hdfs_documents_path="/data"):
    """Safely extract titles from documents in HDFS"""
    print(f"Reading document titles from HDFS: {hdfs_documents_path}")
    titles = {}
    
    try:
        # List all files in the directory
        result = subprocess.run(
            ["hdfs", "dfs", "-ls", hdfs_documents_path],
            stdout=subprocess.PIPE,
            encoding="utf-8",
            check=True
        )
        
        # Process each document file
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) < 8:  # Skip headers or invalid lines
                continue
                
            file_path = parts[-1]  # Last part is the file path
            if not file_path.endswith(".txt"):
                continue
                
            # Extract doc_id from filename (part before the first underscore)
            doc_id = os.path.basename(file_path).split("_")[0]
            
            title = os.path.basename(file_path).split("_", 1)[1].replace("_", " ").split(".")[0]
            titles[doc_id] = title
            print(f"Extracted title for {doc_id}: {title}")

        print(f"Extracted {len(titles)} titles from HDFS")

    except subprocess.CalledProcessError as e:
        print(f"Error reading HDFS: {e}")

    return titles

def process_stage1_output(session, lines, titles):
    for line in lines:
        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue
        key, count = parts
        count = int(count)

        if key.startswith("DOCLEN_"):
            doc_id = key.replace("DOCLEN_", "")
            title = titles.get(doc_id, "")
            session.execute(
                "INSERT INTO doc_stats (doc_id, doc_length, title) VALUES (%s, %s, %s)",
                (doc_id, count, title)
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

def update_doc_titles(session, titles):
    """Update document titles in Cassandra"""
    print("Updating document titles in Cassandra...")
    prepared_stmt = session.prepare("UPDATE doc_stats SET title = ? WHERE doc_id = ?")
    
    for doc_id, title in titles.items():
        try:
            session.execute(prepared_stmt, (title, doc_id))
        except Exception as e:
            print(f"Error updating title for {doc_id}: {e}")
    
    print(f"Updated {len(titles)} document titles")

def main():
    session = init_cassandra()
    
    print("Reading document titles from HDFS...")
    titles = read_document_titles()
    
    print("Processing stage 1 output...")
    stage1_lines = read_hdfs_output("/tmp/index/output1")
    process_stage1_output(session, stage1_lines, titles)
    
    print("Processing stage 2 output...")
    stage2_lines = read_hdfs_output("/tmp/index/output2")
    process_stage2_output(session, stage2_lines)
    
    print("Indexing complete!")

if __name__ == "__main__":
    main()
