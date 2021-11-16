from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.utils import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import logging
import sys, os, json
from pyspark.sql import functions as F
import boto3
from botocore.errorfactory import ClientError
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import subprocess
import asyncio
from asyncio.subprocess import PIPE
import datetime
from pytz import timezone
from dqf_utils import load_from_hdfs, load_from_table, getlogger, check_if_s3_path_exists, execute_cmd, run_async_subprocess, hash_func
s3 = boto3.client('s3')


log = getlogger("Data Quality Framework")
log.info("Start DQ processor")
logging.getLogger("org.apache.spark").setLevel(logging.ERROR)


def count_check(src_df, dest_df, table_name, log):
    src_count = src_df.count()
    dest_count = dest_df.count()
    if src_count != dest_count:
        log.error("The counts of the data at source and target for table {} does not match".format(table_name))
        diff_percent = (src_count-dest_count)/src_count
        log.error("table_name = {} | difference_percentage = {} | source_count = {} | destination_count = {}".format(table_name, diff_percent, src_count, dest_count))
    elif src_count == dest_count:
        log.info("The table {} passed the count check".format(table_name))
        log.info("table_name = {} | count = {}".format(table_name, dest_count))


def data_check(src_df, dest_df, table_name, log):
    try:
        id_cols = src_df.columns
        if src_df and dest_df:
            src_hash_df = hash_func(table_name, src_df, id_cols, log)
            dest_hash_df = hash_func(table_name, dest_df, id_cols, log)
            src_hash = src_hash_df.select("keyhash")
            dest_hash = dest_hash_df.select("keyhash")
            variance = src_hash.join(dest_hash, ['keyhash'], how='left_anti')
            if variance.count() > 0:
                log.error("Data check failed for the table {} with the variance of {}".format(table_name, variance.count()))
            else:
                log.info("Data check passed for the table {}".format(table_name))
        else:
            if src_df.head(1).isEmpty():
                log.error("Table {} doesn't exists in the source location.".format(table_name))
            elif dest_df.head(1).isEmpty():
                log.error("Table {} doesn't exists in the destination location.".format(table_name))
    except Exception as e:
        log.error(e)
        log.error("Unexpected runtime exception occured during the data check for table {}.".format(table_name))



def run_check(src_type, src, dest_type, dest, config_file_path, tranche):
    data =[]
    try:
        with open (config_file_path) as json_file:
            data = json.load(json_file)
    except Exception as e:
        log.error(e)
        log.error("Run time exception while loading the config file at {} ".format(config_file_path))
        return
    
    try:
        table_list = data[tranche]
        log.info(f"Table list being executed: {tranche}")
        if len(table_list) != 0:
            for table_name in table_list:
                if src_type.lower() == "ff":
                    src_df = load_from_hdfs(src, spark, table_name, log)
                elif src_type.lower() == "db":
                    src_df = load_from_table(spark, table_name, log, src)
                if dest_type.lower() == "ff":
                    dest_df = load_from_hdfs(dest, spark, table_name, log)
                elif dest_type.lower() == "db":
                    dest_df = load_from_table(spark, table_name, log, dest)
                count_check(src_df, dest_df, table_name, log)
                data_check(src_df, dest_df, table_name, log)
        log.info("End DQF check")
    except Exception as e:
        log.error(e)
        log.error("Unexpected runtime exception occured during the check for table {}. Continue with processing the next table.".format(table_name))



if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Argument Validation: Excepted 4 command line parameters. source and destination paths and tables_tranche to be used./n For example: dqf_processor.py db connections={'username':'user1', 'password':'pass1', 'hostname':'jdbc:mysql://localhost:3306/uoc2', 'driver':'com.mysql.jdbc.Driver'} ff s3://bucket_name2/folder2/ /configuration/eng/table_list.json grp_a. Exit program.")
        sys.exit(0)
    src_type = sys.argv[1]
    src = sys.argv[2]
    dest_type = sys.argv[3]
    dest = sys.argv[4]
    config_file_path = sys.argv[5]
    tranche = sys.argv[6]



#----------------------- C O N F I G U R A T I O N -----------------------#
# Create Spark context with Spark configuration
conf = SparkConf().setAppName("Spark Data Quality Framework")
conf.set("spark.executor.memoryOverhead", "4096")
conf.set("spark.driver.memoryOverhead", "4096")
conf.set("spark.dynamicAllocation.executorIdleTimeout", "60")
conf.set("spark.dynamicAllocation.minExecutors", "5")
conf.set("spark.dynamicAllocation.initialExecutors", "5")
conf.set("spark.sql.shuffle.partitions", "200")
conf.set("spark.shuffle.compress", "true")
conf.set("spark.shuffle.partitions", "500")
conf.set("spark.default.parallelism", "500")
conf.set("spark.shuffle.file.buffer", "64k")
conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
conf.set("spark.yarn.maxAppAttempts", 1)
conf.set("spark.yarn.max.executor.failures", 100)
conf.set("spark.hadoop.mapred.output.compress", "true")
conf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
conf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")
conf.set("spark.scheduler.mode", "FAIR")
conf.set("spark.sql.broadcastTimeout", 36000)
conf.set("spark.sql.parquet.writeLegacyFormat", "true")
conf.set("spark.scheduler.listenerbus.eventqueue.capacity", 50000)
conf.set("spark.driver.maxResultSize", "4g")
conf.set("spark.scheduler.listernerbus.eventqueue.size", 50000)

conf.set("spark.sql.parquet.writeLegacyFormat", "true")
spark = SparkSession. \
        builder. \
        config(conf=conf). \
        enableHiveSupport(). \
        getOrCreate()
sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")
sc.setLogLevel("ERROR")
run_check(src_type, src, dest_type, dest, config_file_path, tranche)


#----------------------- P R O C E S S I N G -----------------------#

spark.stop()