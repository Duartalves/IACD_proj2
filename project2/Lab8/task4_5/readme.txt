hdfs dfs -put customer-orders.csv /user/hadoop/input/

spark-submit --master local sort_total_customers.py

hdfs dfs -cat /user/hadoop/output/sorted_customer_spending/part-*