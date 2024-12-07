hdfs dfs -put /home/user/data/1800.csv /user/hadoop/input/

spark-submit --master local min_temperature.py

hdfs dfs -cat /user/hadoop/output/min_temperature_per_city/part-*