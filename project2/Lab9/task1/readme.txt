spark-submit --master local min_temperature.py

hdfs dfs -cat /user/hadoop/output/min_temperature_per_city9/part-*