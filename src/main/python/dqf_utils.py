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

def getlogger(name, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers:
        pass
    else:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger


def execute_cmd(cmd, log):
    try:
        log.info("Begin executing the command. Command is {} ".format(cmd))
        myProcess = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = myProcess.communicate()
        log.info("End executing the command")
        return out
    except Exception as e:
        log.error("Exception while executing the command {} ".format(cmd))
        log.error(e)
        raise e


async def read_io_stream(stream, display):
    output = []
    while True:
        line = await stream.readline()
        if not line:
            break
        output.append(line)
        display(line)
    return b''.join(output)


async def run_os_subprocess(*s3distcmd):
    child_process = await asyncio.create_subprocess_exec(*s3distcmd, stdout=PIPE, stderr=PIPE)
    try:
        stdout, stderr = await asyncio.gather(read_io_stream(child_process.stdout, sys.stdout.buffer.write), read_io_stream(child_process.stderr, sys.stderr.buffer.write))
    except Exception:
        child_process.kill()
        raise
    finally:
        return_code = await child_process.wait()
    return return_code, stdout, stderr


def run_async_subprocess(*args):
    loop = asyncio.get_event_loop()
    if loop.is_closed():
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
    return_code, stdout, stderr = loop.run_until_complete(run_os_subprocess(*args))
    loop.close()
    return return_code, stdout, stderr


def check_if_s3_path_exists(s3, bucket, prefix):
    try:
        response = s3.list_objects(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        if "Contents" in response:
            if len(response["Contents"]) > 1:
                return True
            if response["Contents"][0]["Size"] > 0:
                return True
        return False
    except ClientError:
        return False


def load_from_hdfs(hdfs_path, schema, spark, table_name, log):
    try:
        table_path = hdfs_path + table_name + "/"
        for file in os.listdir(table_path):
            if file.endswith(".parquet"):
                file_type = "parquet"
                break
            elif file.endswith(".csv"):
                file_type = "csv"
                break
            elif file.endswith(".json"):
                file_type = "json"
                break
            else:
                file_type = file.split(".")[-1]
        if(file_type == "parquet"):
            df = spark.read.load(table_path, schema = schema)
            return df
        elif(file_type == "csv"):
            df = spark.read.format("csv").schema(schema).option("header", "true").load(table_path)
            return df
        elif(file_type == "json"):
            df = spark.read.json(table_path, schema = schema)
            return df
        else:
            log.error("File format '.{}' found in path {} for table {} is not supported".format(file_type, hdfs_path, table_name))
        return None
    except Exception as ex:
        log.error("Error occurred in loading data from hdfs for the path {} {}".format(hdfs_path, ex))
        return None

def load_from_table(spark, table_name, log, **connection):
    try:
        username = connection["username"]
        password = connection["password"]
        hostname = connection["hostname"]
        driver = connection["driver"]
        df = spark.read.jdbc(hostname, table=table_name, properties={"user": username,"password": password ,"driver": driver})
        return df
    except Exception as ex:
        log.error("Error occurred in loading data from table {} {}".format(table_name, ex))
        return None


def hash_func(table_name, df, id_cols, log):
    try:
        hash_df = df.withColumn("keyhash", sha2(concat_ws("||", *id_cols), 256))
        log.info("Successfully added the hash column for table {}".format(table_name))
        return hash_df
    except Exception as ex:
        log.error("Error occurred in generating hash for table {} {}".format(table_name, ex))
        return None