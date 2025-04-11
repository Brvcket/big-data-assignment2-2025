#!/usr/bin/env python3
import sys
import re
from collections import Counter

def tokenize(text):
    return re.findall(r'\w+', text.lower())

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue
        doc_id, text = parts
        tokens = tokenize(text)
        counter = Counter(tokens)
        for token, count in counter.items():
            print(f"{token}::{doc_id}\t{count}")
        print(f"DOCLEN_{doc_id}\t{len(tokens)}")

if __name__ == '__main__':
    main()
