hdfs dfs -put Marvel+Graph /user/hadoop/input/
hdfs dfs -put Marvel+Names /user/hadoop/input/

spark-submit --master local marvel_superheroes.py

hdfs dfs -cat /user/hadoop/output/superhero_popularity/part-*