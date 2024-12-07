hdfs dfs -put /home/user/data/Book.txt /user/hadoop/input/

spark-submit --master local sort_words.py

hdfs dfs -cat /user/hadoop/output/sorted_words/part-*