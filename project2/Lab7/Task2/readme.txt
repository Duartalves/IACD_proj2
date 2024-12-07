Upload the File to HDFS:
hadoop fs -put 1800.csv /home/hadoop/input/

hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -input /home/hadoop/input/1800.csv \
    -output /home/hadoop/output/min_temperature_per_capital \
    -mapper /home/hadoop/scripts/mapper.py \
    -reducer /home/hadoop/scripts/reducer.py \
    -file /home/hadoop/scripts/mapper.py \
    -file /home/hadoop/scripts/reducer.py

hadoop fs -get /home/hadoop/output/min_temperature_per_capital/part-00000 ./min_temperature_per_capital.txt