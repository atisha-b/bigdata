import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell,mysql:mysql-connector-java:5.1.49'

import findspark
findspark.init('/home/ubuntu/spark/')                                                                                                                         
from pyspark import SparkContext                                                                                        
from pyspark.sql import SparkSession                                                                                    
from pyspark.streaming import StreamingContext                                                                          
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

TOPIC = 'trans'
BOOTSTRAP_SERVER = '18.222.161.239:9092'

def save_to_hive(df, tbl_name):
        global ss
        df.show()
        print("sending to hive table ")
        df.write.saveAsTable(name=tbl_name,format='hive',mode='append')


                                                                                                                                               
if _name_ == "_main_":
                                                                                                                        
        ss = SparkSession.builder.appName("Spending Analysis").master("local[*]").config("spark.jars","/home/ubuntu/kafka-clients-3.4.0.jar").config("spark.sql.warehouse.dir", "/user/hive/warehouse").config("hive.metastore.uris", "thrift://18.222.161.239:9083").enableHiveSupport().getOrCreate()                                                                                                  
                                                                                                                        
        ss.sparkContext.setLogLevel('ERROR')   

        trans_df = ss.readStream.format("kafka").option("kafka.bootstrap.servers",BOOTSTRAP_SERVER).option("subscribe",TOPIC).option("startingOffsets","latest").load()
        account_df =  ss.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url","jdbc:mysql://127.0.0.1:3306/berka?user=root&password=admin&useUnicode=true&characterEncoding=UTF-8").option("query","select account_id, district_id, frequency from account").load()
        client_df =  ss.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url","jdbc:mysql://127.0.0.1:3306/berka?user=root&password=admin&useUnicode=true&characterEncoding=UTF-8").option("query","select client_id, district_id, gender, age, age_levels from client").load()
        district_df =  ss.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url","jdbc:mysql://127.0.0.1:3306/berka?user=root&password=admin&useUnicode=true&characterEncoding=UTF-8").option("query","select district_id, district_name, region_name from district").load()

        print("Printing schema of trans_df")
        trans_df.printSchema()

        account_df_renamed = account_df.withColumnRenamed("account_id", "account_id_account_df")

        trans_df1 = trans_df.selectExpr("CAST(value AS STRING)","timestamp")

        trans_schema = StructType().add("trans_id",StringType()).add("account_id",StringType()).add("date",StringType()).add("type",StringType()).add("operation",StringType()).add("amount",StringType()).add("balance",StringType()).add("k_symbol",StringType()).add("bank",StringType()).add("account",StringType())
        
        trans_df2 = trans_df1.select(from_json(col("value"),trans_schema).alias("transactions"),"timestamp")

        trans_df3 = trans_df2.select("transactions.*","timestamp")

        trans_account_joined_df = trans_df3.join(account_df_renamed, trans_df3.account_id == account_df_renamed.account_id_account_df, "inner")
        joined_df = trans_account_joined_df.join(client_df,"district_id", "inner")

        joined_df.show()

        #monthly transactions
         
        trans_df4 = trans_df3.withColumn("transaction_date", to_date(col("date"), "yyMMdd"))
        trans_df5 = trans_df4.withColumn("transaction_month", month(col("transaction_date")))

        monthly_transaction_trend = trans_df5.groupBy("transaction_month").agg({'amount': 'sum', 'trans_id': 'count'})\
                                     .select("transaction_month", col("count(trans_id)").alias("transaction_count"),
                                             col("sum(amount)").alias("transaction_amount"))


        monthly_transaction_trend.printSchema()
        monthly_transaction_trend.writeStream.trigger(processingTime='10 seconds').outputMode("update").foreachBatch(lambda current_df, epoc_id:save_to_hive(current_df, "default.monthly_trend")).start()

        #yearly transactions
        trans_df6 = trans_df3.withColumn("transaction_date", to_date(col("date"), "yyMMdd"))
        trans_df7 = trans_df6.withColumn("transaction_year", year(col("transaction_date")))

        yearly_transaction_trend = trans_df7.groupBy("transaction_year").agg({'amount': 'sum', 'trans_id': 'count'})\
                                     .select("transaction_year", col("count(trans_id)").alias("transaction_count"),
                                             col("sum(amount)").alias("transaction_amount"))


        yearly_transaction_trend.printSchema()
        yearly_transaction_trend.writeStream.trigger(processingTime='10 seconds').outputMode("update").foreachBatch(lambda current_df, epoc_id:save_to_hive(current_df, "default.yearly_trend")).start()

        #average transactions by gender, age_levels

        avg_trans = joined_df.groupBy("age_levels", "gender").agg({'amount': 'avg'}).\
                                    select("age_levels", "gender", col("avg(amount)").alias("avg_transactions"))
        avg_trans.printSchema()
        avg_trans.writeStream.trigger(processingTime='10 seconds').outputMode("update").foreachBatch(lambda current_df, epoc_id:save_to_hive(current_df, "default.trans_by_age_gender")).start()

        # transactions by operation
        trans_agg = trans_df3.groupBy("operation").agg({'amount':'sum'}).select("operation",col("sum(amount)").alias("total"))

        trans_agg.printSchema()
        

        trans_write_stream = trans_agg.writeStream.outputMode("update").foreachBatch(lambda current_df, epoc_id:save_to_hive(current_df, "default.ops_totals")).start()
        trans_write_stream.awaitTermination(
