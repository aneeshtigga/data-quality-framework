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
from configparser import ConfigParser
from pyspark.sql.types import StructField
from dqf_utils import load_from_hdfs, load_from_table, getlogger, check_if_s3_path_exists, execute_cmd, run_async_subprocess, hash_func
s3 = boto3.client('s3')


log = getlogger("Data Quality Framework")
log.info("Start DQ processor")
logging.getLogger("org.apache.spark").setLevel(logging.ERROR)


parser = ConfigParser()
parser.read('../../../configuration/eng/tables.ini')


def count_check(src_df, dest_df, table_name, log):
    log.info("########### Performing Count Check ###########")
    src_count = src_df.count()
    dest_count = dest_df.count()
    if src_count != dest_count:
        log.error("The counts of the data at source and target for table \"{}\" does not match".format(table_name))
        diff_percent = (src_count-dest_count)/src_count
        log.error("table_name = {} | difference_percentage = {} | source_count = {} | destination_count = {}".format(table_name, diff_percent, src_count, dest_count))
    elif src_count == dest_count:
        log.info("The table \"{}\" passed the count check".format(table_name))
        log.info("table_name = {} | count = {}".format(table_name, dest_count))


def data_check(src_df, dest_df, table_name, log):
    log.info("########### Performing Data Check ###########")
    try:
        id_cols = []
        with open(f'../../../configuration/eng/schema/{table_name}.json') as f:
            table_meta = json.load(f)['columns']
        toc = table_meta['fields']
        for i in toc:
            if i['id_flag'] == 1:
                id_cols.append(i['name'])
        if src_df and dest_df:
            src_hash_df = hash_func(table_name, src_df, id_cols, log)
            dest_hash_df = hash_func(table_name, dest_df, id_cols, log)
            src_hash = src_hash_df.select("keyhash")
            dest_hash = dest_hash_df.select("keyhash")
            variance = src_hash.join(dest_hash, ['keyhash'], how='left_anti')
            if variance.count() > 0:
                log.error("Data check failed for the table \"{}\". {} rows from source are missing in target".format(table_name, variance.count()))
            else:
                log.info("Data check passed for the table \"{}\". All rows from the source are present in target".format(table_name))
        else:
            if src_df.head(1).isEmpty():
                log.error("Table \"{}\" doesn't exists in the source location.".format(table_name))
            elif dest_df.head(1).isEmpty():
                log.error("Table \"{}\" doesn't exists in the destination location.".format(table_name))
    except Exception as e:
        log.error(e)
        log.error("Unexpected runtime exception occured during the data check for table \"{}\".".format(table_name))



def run_check(tlist):    
    try:
        table_list = tlist
        log.info(f"Table list being executed: {tlist}")
        if len(table_list) != 0:
            for table_name in table_list:
                log.info(f"-----------Processing table \"{table_name}\"-----------")
                with open(f'../../../configuration/eng/schema/{table_name}.json') as f:
                    table_meta = json.load(f)
                typesOfChecks = table_meta['__meta__']['typesOfChecks']
                if len(typesOfChecks) == 0:
                    log.error(f'No checks defined for the table \'{table_name}\' in the schema file. Add the checks under \'typesOfChecks\'')
                else:
                    cols = table_meta['columns']
                    schema = StructType.fromJson(cols)
                    src_host = parser.get(table_name, 'src_host')
                    src_type = parser.get(table_name, 'src_type')
                    src = parser.get(table_name, 'src_url')
                    dest_host = parser.get(table_name, 'dest_host')
                    dest_type = parser.get(table_name, 'dest_type')
                    dest = parser.get(table_name, 'dest_url')
                    if src_host.lower() == "cloud-s3":
                        AWS_ACCESS_KEY = parser.get(table_name, 'src_aws_access_key_id')
                        AWS_SECRET_KEY = parser.get(table_name, 'src_aws_secret_access_key')
                        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
                        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)
                    if src_type.lower() == "ff":
                        src_df = load_from_hdfs(src, schema, spark, table_name, log)
                        src_df.printSchema()
                    elif src_type.lower() == "db":
                        src_df = load_from_table(spark, table_name, log, src)
                        src_df.printSchema()
                    if dest_host.lower() == "cloud-s3":
                        AWS_ACCESS_KEY = parser.get(table_name, 'dest_aws_access_key_id')
                        AWS_SECRET_KEY = parser.get(table_name, 'dest_aws_secret_access_key')
                        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
                        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)
                    if dest_type.lower() == "ff":
                        dest_df = load_from_hdfs(dest, schema, spark, table_name, log)
                        dest_df.printSchema()
                    elif dest_type.lower() == "db":
                        dest_df = load_from_table(spark, table_name, log, dest)
                        dest_df.printSchema()
                    for i in typesOfChecks:
                        if i == 'count_check':
                            count_check(src_df, dest_df, table_name, log)
                        elif i == 'data_check':
                            data_check(src_df, dest_df, table_name, log)
        log.info("End DQF check")
    except Exception as e:
        log.error(e)
        log.error("Unexpected runtime exception occured during the check for table {}. Continue with processing the next table.".format(table_name))



if __name__ == "__main__":
    tlist = parser.sections()
    if len(tlist) == 0:
        log.error("Either the configuration/eng/tables.ini file is empty or not generated. Use create_tables_config.py under script/schema_generator path to generate the file")
        sys.exit(0)


#----------------------- C O N F I G U R A T I O N -----------------------#
# Create Spark context with Spark configuration
conf = SparkConf().setAppName("Spark Data Quality Framework")
conf.set("spark.executor.memoryOverhead", "4096")
conf.set("spark.driver.memoryOverhead", "4096")
conf.set("spark.dynamicAllocation.executorIdleTimeout", "60")
conf.set("spark.dynamicAllocation.minExecutors", "5")
conf.set("spark.dynamicAllocation.initialExecutors", "5")
conf.set("spark.sql.shuffle.partitions", "16")
conf.set("spark.shuffle.compress", "true")
conf.set("spark.shuffle.partitions", "16")
conf.set("spark.default.parallelism", "8")
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
run_check(tlist)


#----------------------- P R O C E S S I N G -----------------------#

spark.stop()