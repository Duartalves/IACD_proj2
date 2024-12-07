#!/usr/bin/env python3
import sys

current_station = None
min_temp = None

# Process each line of input
for line in sys.stdin:
    station_id, temp = line.strip().split("\t")
    temp = int(temp)

    if current_station == station_id:
        min_temp = min(min_temp, temp)
    else:
        if current_station is not None:
            # Output the minimum temperature for the previous station
            print(f"{current_station}\t{min_temp}")
        current_station = station_id
        min_temp = temp

# Output the last station
if current_station is not None:
    print(f"{current_station}\t{min_temp}")
