import mysql.connector as mysql
import csv
import sys
import logging as log
from datetime import date
import sql_statements as sql


DB_TABLE = sys.argv[1]
LOAD_TYPE = sys.argv[2]   
DATE_PREFIX = sys.argv[3]

db_objects = {} # dictionary to hold db connection parameters

def __logger(file, log_level='WARNING'):
    assert (log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']), ("invalid log level")
    
    if log_level == 'DEBUG':
        level = log.DEBUG
    elif log_level == 'INFO':
        level = log.INFO
    elif log_level == 'WARNING':
        level = log.WARNING
    elif log_level == 'ERROR':
        level = log.ERROR
    else:
        level = log.CRITICAL
    
    log.basicConfig(filename=file, filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', level=level)
    log.info("starting the logger...")


def load_db_credentials(file_path):
    """
    takes a file and load db credentials into the db_objects dict
    """

    try:
        with open(file_path, "r") as param:
            for line in param:
                name, val = line.partition("=")[::2]
                db_objects[name.strip()] = str(val).strip()
    except FileNotFoundError as err:
        log.error(f"unable to load database credentials from file {file_path}: {str(err)}")
        sys.exit(1)



def main() -> Exception:
    """ 
    retrives records from mysql db and writes to csv files, 
    each file contains max 100,000 records and returns any exception
    """
    file_path = "./config/params.ini"
    load_db_credentials(file_path)

    try:
        db = mysql.connect(host=db_objects["host"], port=int(db_objects["port"]), user=db_objects["user"], password=db_objects["password"], db=db_objects["database"], connect_timeout=int(db_objects["timeout_seconds"]))
    except mysql.DatabaseError as err:
        log.error(f"unable to connect to database: {str(err)}")
        return err

    max_records_per_fetch = 100000
        
    try:
        cursor = db.cursor()
        
        cursor.execute(sql.sql_statements["sql_db_bakery_" + DB_TABLE])
        
        # get column headers
        col_names = [column[0] for column in cursor.description]
        
        file_num = 0
        date_split = DATE_PREFIX.strip().split("-")
        ds_year, ds_month, ds_day = date_split[0], date_split[1], date_split[2]

        while True:     # loop until all records are written to csv file
            rows = cursor.fetchmany(size=max_records_per_fetch)
            if not rows:
                break
            
            if LOAD_TYPE == "full":
                file_name = f"./out/{DB_TABLE}/{DATE_PREFIX}_part{file_num}.csv"
            else:
                file_name = f"./out/{DB_TABLE}/{ds_year}/{ds_month}/{ds_day}/{DATE_PREFIX}_part{file_num}.csv"
                
            with open(file_name, "w+", encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file, delimiter="|")
                writer.writerow(col_names)

                for row in rows:
                    writer.writerow(row)
            file_num += 1
    except Exception as err:
        log.error(f"program exception: {str(err)}")
        return err
    finally:
        db.close()

    return None
    

if __name__ == "__main__":
    log_level = sys.argv[4]
    log_file = f"./log/{date.today()}_mysql-to-s3.log"
    __logger(log_file, log_level)
    err = main()
    if err is not None:
        log.error(f"exiting from main with error: {str(err)}")
        sys.exit(1)

