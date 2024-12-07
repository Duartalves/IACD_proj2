#!/usr/bin/env python3
import sys
from collections import defaultdict

# Dictionary to store total amount per customer
customer_totals = defaultdict(float)

# Read key-value pairs from standard input
for line in sys.stdin:
    # Strip whitespace and split the line
    line = line.strip()
    customer_id, amount = line.split('\t')

    # Accumulate the total amount for each customer
    customer_totals[customer_id] += float(amount)

# Sort customers by total amount spent in descending order
sorted_totals = sorted(customer_totals.items(), key=lambda x: x[1], reverse=True)

# Output the sorted results
for customer_id, total in sorted_totals:
    print(f"{customer_id}\t{total}")
