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
from boto3 import Session
import botocore
import threading


DB_TABLE = sys.argv[1]
LOAD_TYPE = sys.argv[2]   
DATE_PREFIX = sys.argv[3]

aws_access_key_id=""
aws_secret_access_key=""

config = {} # dictionary to hold db connection parameters
BUCKET = "data-load-squeakysimple"

class uploadFileS3(threading.Thread):
    def __init__(self, out_dir, file):
        threading.Thread.__init__(self)
        self.out_dir = out_dir
        self.file = file

    def run(self):
        process_file_s3(self.out_dir, self.file)


def set_logger(log_level='ERROR'):
    """
    Module level logger
    """

    assert (log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']), ("invalid log level")  # input validation, sys.argv[4]
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
    takes a file and load db credentials into the db_objects dict
    """

    try:
        with open(file, "r") as param:
            for line in param:
                name, val = line.partition("=")[::2]
                config[name.strip()] = str(val).strip()
    except FileNotFoundError as err:
        logging.error(f"unable to load config file {file}: {str(err)}")
        sys.exit(1)

def create_session_s3(aws_access_key_id, aws_secret_access_key): 
    return Session(aws_access_key_id, aws_secret_access_key)


def create_client_s3(aws_access_key_id, aws_secret_access_key): 
    return boto3.client("s3", aws_access_key_id, aws_secret_access_key)   


def load_config_from_s3(s3_bucket_name, key):
    """
    downloads the config file from S3 and loads its contents into config dict
    """
    
    session = create_session_s3(aws_access_key_id, aws_secret_access_key)
    s3 = session.resource("s3")
    
    try:
        s3.Bucket(s3_bucket_name).download_file(key, os.path.basename(key))
        logger.info(f"successfully downloaded config file from s3 bucket {s3_bucket_name}")
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "404":
            logger.error(f"the object doesn't exist in s3 bucket {s3_bucket_name}")
        else:
            raise
        sys.exit(1)

    set_config(key)

def file_destination():
    if LOAD_TYPE == "full":
        out_dir = f"out/{DB_TABLE}"
        s3_dest = f"s3://data-load-squeakysimple/{DB_TABLE}/"
    else:
        run_date_split = DATE_PREFIX.split('-') 
        year, month, day = run_date_split[0], run_date_split[1], run_date_split[2]
        out_dir = f"out/{DB_TABLE}/{year}/{month}/{day}"
        s3_dest = f"s3://data-load-squeakysimple/{DB_TABLE}/{year}/{month}/{day}/"

    return out_dir, s3_dest

def prepare_outdir() -> str:
    out_dir, _ = file_destination()
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    
    os.makedirs(out_dir)

    return out_dir

def gz_compress_csv(file_name):
    with open(file_name, "rb") as csv_in:
        with gzip.open(f"{file_name}.gz", "wb") as gz_out:
            shutil.copyfileobj(csv_in, gz_out)
    

def upload_file_to_s3(local_path, s3_key):
    #client = create_client_s3(aws_access_key_id, aws_secret_access_key)
    #s3 = client.resource("s3")
    client = boto3.client("s3", aws_access_key_id="",
            aws_secret_access_key="")
    
    try:
        client.upload_file(local_path, BUCKET, s3_key)
    except Exception as err:
        logger.error(f"failed to upload file {local_path} to S3 bucket {BUCKET}, with error {str(err)}")
        sys.exit(1)

def process_file_s3(out_dir, file):
    file_name = f"{out_dir}/{file}"
    gz_compress_csv(file_name)
    os.remove(file_name)
    s3_key = f"{DB_TABLE}/" + file + ".gz"
    upload_file_to_s3(file_name + ".gz", s3_key)



def main() -> Exception:
    """ 
    retrives records from mysql db and writes to csv files, 
    each file contains max 100,000 records and returns any exception
    """
    # file_path = "./params/params.ini"
    # load_db_credentials(file_path)
    load_config_from_s3("sb-mysql-to-s3", "params.ini")
    logger.debug("successfully loaded the config")
    out_dir = prepare_outdir()
    logger.debug("prepared out dir")

    
    try:
        db = mysql.connect(host=config["host"], port=int(config["port"]), user=config["user"], password=config["password"], db=config["database"], connect_timeout=int(config["timeout_seconds"]))
    except mysql.DatabaseError as err:
        logger.error(f"unable to connect to database: {str(err)}")
        return err
        
    try:
        cursor = db.cursor()
        
        cursor.execute(sql.sql_statements["sql_db_bakery_" + DB_TABLE])
        logger.info(f"total rows processed = {cursor.rowcount}")
        
        # get column headers
        col_names = [column[0] for column in cursor.description]
        
        file_num = 0
        #date_split = DATE_PREFIX.strip().split("-")
        #ds_year, ds_month, ds_day = date_split[0], date_split[1], date_split[2]

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
        return err
    finally:
        db.close()

    files = []
    for file in os.listdir(out_dir):
        files.append(file)

    for f in files:
        thread = uploadFileS3(out_dir, f)
        #thread = uploadFileS3(file_name + ".gz", s3_key)
        thread.start()

    # for file in os.listdir(out_dir):       
    #     file_name = f"{out_dir}/{file}"
    #     gz_compress_csv(file_name)
    #     os.remove(file_name)
    #     s3_key = "cakes/" + file + ".gz"
    #  #   print(file_name, BUCKET, s3_key)
    #     #logger.debug("starting thread")

    #     # upload_file_to_s3(file_name + ".gz", s3_key)
    #     thread = uploadFileS3(file_name + ".gz", s3_key)
    #     thread.start()
    #    thread.getName()
    #    thread.join()
    print(threading.active_count())
    # print(threading.currentThread())
    while threading.active_count() > 1:
       continue

    # if os.path.exists(out_dir):
    #     shutil.rmtree(out_dir)

    return None
    

if __name__ == "__main__":
#   log_file = f"./log/{date.today()}_mysql-to-s3.log"
    set_logger(sys.argv[4])
    err = main()
    if err is not None:
        logger.error(f"exiting from main with error: {str(err)}")
        sys.exit(1)

