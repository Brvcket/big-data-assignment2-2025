#!/usr/bin/env python3
import sys
import math

def main():
    current_term = None
    current_docs = {}
    total_docs = 0
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        key, value = line.split("\t", 1)
        
        if key == "TOTAL_DOCS":
            total_docs += 1
            continue
        
        if key != current_term:
            if current_term is not None and current_term != "TOTAL_DOCS":
                doc_count = len(current_docs)
                idf = math.log(max(1, total_docs / max(1, doc_count))) if total_docs > 0 else 0
                print(f"{current_term}\t{doc_count}\t{idf:.6f}")
                
            current_term = key
            current_docs = {}
        
        doc_info = value.split(":", 1)
        if len(doc_info) == 2:
            doc_id, tf = doc_info
            current_docs[doc_id] = tf

    if current_term is not None and current_term != "TOTAL_DOCS":
        doc_count = len(current_docs)
        idf = math.log(max(1, total_docs / max(1, doc_count))) if total_docs > 0 else 0
        print(f"{current_term}\t{doc_count}\t{idf:.6f}")

if __name__ == '__main__':
    main()