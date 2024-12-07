#!/usr/bin/env python3
import sys

# Read input line by line
for line in sys.stdin:
    # Strip whitespace and split the line
    line = line.strip()
    parts = line.split(',')

    # Extract Customer_ID and Amount
    customer_id = parts[0]
    amount = parts[2]

    # Emit key-value pair: Customer_ID\tAmount
    print(f"{customer_id}\t{amount}")
