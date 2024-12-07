Upload the File to HDFS:
hadoop fs -put Book.txt /home/hadoop/input/

hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -input /user/hadoop/input/Book.txt \
    -output /user/hadoop/output/word_frequency_sorted \
    -mapper /home/hadoop/scripts/mapper.py \
    -reducer /home/hadoop/scripts/reducer.py \
    -file /home/hadoop/scripts/mapper.py \
    -file /home/hadoop/scripts/reducer.py

hadoop fs -get /user/hadoop/output/word_frequency_sorted/part-00000 ./word_frequency_sorted.txt