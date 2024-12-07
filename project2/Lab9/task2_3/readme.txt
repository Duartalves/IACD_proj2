spark-submit --master local sort_words.py

hdfs dfs -cat /user/hadoop/output/sorted_words9/part-*