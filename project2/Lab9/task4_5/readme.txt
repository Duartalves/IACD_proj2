spark-submit --master local sort_total_customers.py

hdfs dfs -cat /user/hadoop/output/sorted_customer_spending9/part-*