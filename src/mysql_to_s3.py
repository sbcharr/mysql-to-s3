#!/usr/bin/env python3

import MySQLdb as mysql # mysql.connector
import csv
import sys
import logging
from datetime import date
import sql_statements as sql
import os
import shutil
import gzip
import boto3
import threading
import time
import platform

print("python version: ", platform.python_version())
# constants
DB_TABLE = sys.argv[1]  # source table name
LOAD_TYPE = sys.argv[2]   # full or incr
DATE_PREFIX = sys.argv[3]   # execution date
CONFIG_S3_BUCKET = sys.argv[4]  # s3 bucket where program config is stored
PARTITION_KEY = sys.argv[5]  # partion key to be used in S3 (only applicable for incr load type, else 'None')
AWS_ACCESS_KEY_ID=sys.argv[6]
AWS_SECRET_ACCESS_KEY=sys.argv[7]


config = {} # dictionary to hold db connection parameters


# class uploadFileS3 abstracts multithreded model to upload files to S3 
class uploadFileS3(threading.Thread):
    def __init__(self, out_dir, s3_dest, file):
        threading.Thread.__init__(self)
        self.out_dir = out_dir
        self.file = file
        self.s3_dest = s3_dest

    def run(self):
        process_file_s3(self.out_dir, self.s3_dest, self.file)


def set_logger(log_level='ERROR'):
    """
    Module level logger
    """

    assert (log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']), ("invalid log level") 
    global logger

    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    log_handler = logging.StreamHandler()

    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(log_format)
    logger.addHandler(log_handler)

    logger.info("starting the logger...")


def set_config(file):
    """
    takes a config file and loads data into config dictionary
    """

    try:
        with open(file, "r") as param:
            for line in param:
                name, val = line.partition("=")[::2]
                config[name.strip()] = str(val).strip()
    except FileNotFoundError as err:
        print(f"unable to load program config file {file}: {str(err)}")
        sys.exit(1)
   

def create_resource_s3(aws_access_key_id, aws_secret_access_key):
    """
    returns an S3 resource
    """
    return boto3.resource("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)


def load_config_from_s3(config_s3_bucket, key):
    """
    downloads the config file from S3 and loads its contents into config dict
    """

    s3_resource = create_resource_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    
    try:
        s3_resource.Bucket(config_s3_bucket).download_file(key, os.path.basename(key))
    except Exception as err:
        print(f"unable to download file from {config_s3_bucket} : {str(err)}")
        sys.exit(1)

    set_config(key)


def file_destination():
    """
    destination file path, 
    returns ouput directory as well s3 destination folder
    """

    if LOAD_TYPE == "full":
        out_dir = f"out/{DB_TABLE}"
        s3_dest_folder = f"{DB_TABLE}/"
    else:
        out_dir = f"out/{DB_TABLE}/{DATE_PREFIX}"
        s3_dest_folder = f"{DB_TABLE}/{PARTITION_KEY}={DATE_PREFIX}/" 

    return out_dir, s3_dest_folder


def prepare_outdir():
    """
    prepares the directory structure on disk,
    returns output directory as well as the s3 destination folder
    """

    out_dir, s3_dest_folder = file_destination()
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    
    os.makedirs(out_dir)

    return out_dir, s3_dest_folder


def gz_compress_csv(file_name):
    """
    compresses file to gzip format and saves to disk
    """

    with open(file_name, "rb") as csv_in:
        with gzip.open(f"{file_name}.gz", "wb") as gz_out:
            shutil.copyfileobj(csv_in, gz_out)
    

def upload_file_to_s3(local_path, s3_key):
    """
    uploads data files to s3,
    returns nothing
    """
    s3_resource = create_resource_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    s3_bucket = config["data_bucket_s3"]
    try:
        s3_resource.meta.client.upload_file(local_path, s3_bucket, s3_key)
    except Exception as err:
        logger.error(f"failed to upload file {local_path} to S3 bucket {s3_bucket}, with error {str(err)}")
        sys.exit(1)


def process_file_s3(out_dir, s3_dest, file):
    """
    prepares and uploads data files to s3,
    returns nothing
    """
    file_name = f"{out_dir}/{file}"
    gz_compress_csv(file_name)
    os.remove(file_name)    # removes the original .csv file after .gz version is created. This is to save disk space on container.
    s3_key = f"{s3_dest}" + file + ".gz"
    upload_file_to_s3(file_name + ".gz", s3_key)


def main():
    """ 
    retrives records from mysql db and writes to csv files, retuns nothing
    """
    
    out_dir, s3_dest_folder = prepare_outdir()
    logger.debug("prepared out dir")

    try:
        db = mysql.connect(host=config["host"], port=int(config["port"]), user=config["user"], password=config["password"], db=config["database"], connect_timeout=int(config["timeout_seconds"]))
    except Exception as err:
        logger.error(f"unable to connect to database: {str(err)}")
        sys.exit(1)
        
    try:
        cursor = db.cursor()
        
        cursor.execute(sql.sql_statements[f"sql_db_bakery_{DB_TABLE}"])
        logger.info(f"total rows processed = {cursor.rowcount}")
        
        # get column headers
        col_names = [column[0] for column in cursor.description]
        file_num = 0

        while True:     # loop until all records are written to csv file
            rows = cursor.fetchmany(size=int(config["max_records_per_fetch"]))
            if not rows:
                break
            
            if LOAD_TYPE == "full":
                file_name = f"{out_dir}/{DB_TABLE}_part{file_num}.csv"
            else:
                file_name = f"{out_dir}/{DATE_PREFIX}_part{file_num}.csv"
                
            with open(file_name, "w+", encoding='utf-8') as csv_out:
                writer = csv.writer(csv_out, delimiter="|")
                writer.writerow(col_names)

                for row in rows:
                    writer.writerow(row)
            file_num += 1
    except Exception as err:
        logger.error(f"program exception: {str(err)}")
        db.close()
        sys.exit(1)
    finally:
        db.close()

    files = []
    for file in os.listdir(out_dir):
        files.append(file)

    for f in files:
        thread = uploadFileS3(out_dir, s3_dest_folder, f)
        thread.start()

    while threading.active_count() > 1:
        time.sleep(.100)
        continue

    return
    

if __name__ == "__main__":
    load_config_from_s3(CONFIG_S3_BUCKET, "params.ini")
    set_logger(config["log_level"])

    # starts main()
    main()

