Upload the File to HDFS:
hadoop fs -mkdir -p /home/hadoop/input
hadoop fs -put fakefriends.csv /home/hadoop/input/

hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -input /home/hadoop/input/fakefriends.csv \
    -output /home/hadoop/output/friends_avg_by_age \
    -mapper /home/hadoop/scripts/mapper.py \
    -reducer /home/hadoop/scripts/reducer.py \
    -file /home/hadoop/scripts/mapper.py \
    -file /home/hadoop/scripts/reducer.py

hadoop fs -get /home/hadoop/output/friends_avg_by_age/part-00000 ./friends_avg_by_age_output.txt