#!/usr/bin/env python3

import sys

current_word = None
current_count = 0

word_dict = {}

# Read lines from the standard input
for line in sys.stdin:
    # Split the line into word and count
    word, count = line.split(',')
    count = int(count)

    # if current word is in the dictionary
    if word in word_dict:
        # increment the count
        current_count = word_dict[word] + count
        # update the dictionary
        word_dict[word] = current_count

    else:
        # add the word to the dictionary
        word_dict[word] = count

# sort the dictionary by number of counts
word_dict = dict(sorted(word_dict.items(), key=lambda item: item[1], reverse=True))

# print the dictionary
print(word_dict)
    
