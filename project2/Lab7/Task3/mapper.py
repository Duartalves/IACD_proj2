#!/usr/bin/env python3
import sys

# Read input line by line
for line in sys.stdin:
    # remove any character that is not 'a-z' + 'A-Z' 
    # pontuation should be removed
    line = ''.join(e for e in line if e.isalnum() or e.isspace())
    # split the line into words
    words = line.split()
    # iterate over the words
    for word in words:
        # print the word and 1
        print(f"{word.lower()},1")