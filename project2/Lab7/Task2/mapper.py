#!/usr/bin/env python3
import sys

# Process each line of input
for line in sys.stdin:
    fields = line.strip().split(",")
    if len(fields) >= 5 and fields[2] == "TMIN":  # Only process TMIN records
        station_id = fields[0]  # Weather station ID
        temperature = int(fields[3])  # Temperature (already multiplied by 10)
        print(f"{station_id}\t{temperature}")
