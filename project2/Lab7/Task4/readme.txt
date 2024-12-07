Upload the File to HDFS:
hadoop fs -put customer-orders.csv /home/hadoop/input/

hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -input /user/hadoop/input/customer-orders.csv \
    -output /user/hadoop/output/total_amount_by_customer \
    -mapper /home/hadoop/scripts/mapper.py \
    -reducer /home/hadoop/scripts/reducer.py \
    -file /home/hadoop/scripts/mapper.py \
    -file /home/hadoop/scripts/reducer.py

hadoop fs -get /user/hadoop/output/total_amount_by_customer/part-00000 ./total_amount_by_customer.txt