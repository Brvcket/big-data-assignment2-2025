#!/usr/bin/env python3
import sys

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        parts = line.split("\t")
        if len(parts) != 2:
            continue
            
        key, count = parts
        count = int(count)
        
        if key.startswith("DOCLEN_"):
            doc_id = key.replace("DOCLEN_", "")
            print(f"TOTAL_DOCS\t{doc_id}")
        else:
            try:
                term, doc_id = key.split("::")
                print(f"{term}\t{doc_id}:{count}")
            except ValueError:
                continue

if __name__ == '__main__':
    main()