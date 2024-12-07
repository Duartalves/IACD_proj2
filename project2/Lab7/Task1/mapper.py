#!/usr/bin/env python3
import sys

# Input comes from standard input
for line in sys.stdin:
    line = line.strip()
    # Split the line into fields
    fields = line.split(",")
    if len(fields) == 4:
        age = fields[2]
        num_friends = fields[3]
        print(f"{age}\t{num_friends}")
