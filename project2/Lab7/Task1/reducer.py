#!/usr/bin/env python3
import sys

current_age = None
current_total = 0
current_count = 0

# Input comes from standard input
for line in sys.stdin:
    line = line.strip()
    age, num_friends = line.split("\t")
    num_friends = int(num_friends)
    
    if current_age == age:
        current_total += num_friends
        current_count += 1
    else:
        if current_age is not None:
            # Output the average for the previous age
            print(f"{current_age}\t{current_total / current_count}")
        current_age = age
        current_total = num_friends
        current_count = 1

# Output the last age's average
if current_age is not None:
    print(f"{current_age}\t{current_total / current_count}")
