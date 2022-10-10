# Author             : Anirudh P M
# Created Date       : 20-09-2022
# Updated By         :
# Updated On         :

import os
import subprocess
from botocore.errorfactory import ClientError
import psycopg2
import boto3
import logging
#import MySQLdb
from datetime import datetime, timedelta
from dotenv import load_dotenv


class GlobalConfig:
    def __init__(self):
        load_dotenv()
        self.start_date = None
        self.end_date = None
        self.base_dir = os.getenv("BASE_DIR")
        self.current_date = datetime.now()
        # Defining the log file name
        file_name = self.base_dir + 'logs/global_' + str(self.current_date.date()) + '.log'
        # Defining the log file format
        logging.basicConfig(filename=file_name, level=logging.DEBUG, format='[%(asctime)s]: %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p', filemode='w')
        self.profile_name = os.getenv("PROFILE_NAME")
        self.dev = boto3.session.Session(profile_name=self.profile_name, region_name=os.getenv("REGION"))  # Creating a session
        self.s3_resource = self.dev.resource('s3')
        self.s3_client = self.dev.client('s3')

    # This function is used to write the log message to log file
    def write_to_log(self, data, type='info'):
        print(data)
        if type == 'error':
            logging.error(data)
        else:
            logging.info(data)

    def return_datasource_id(self):
        return {
            "datasource_id": os.getenv("datasource_id")
        }

    """ This function returns the redshift connection details """

    def get_redshift_credentials(self):
        return {"redshift_dbname": os.getenv('REDSHIFT_DBNAME'),
                "redshift_host": os.getenv('REDSHIFT_HOST'),
                "redshift_port": os.getenv('REDSHIFT_PORT'),
                "redshift_user": os.getenv('REDSHIFT_USER'),
                "redshift_password": os.getenv('REDSHIFT_PASSWORD')}

    def get_bucket_name(self):
        return os.getenv("bucket_name")

    def get_env(self):
        return os.getenv("get_env")

    def get_client_config_file(self):
        return os.getenv("CLIENT_CONFIG_FILE")

    def get_query_config_file(self):
        return os.getenv("QUERY_CONFIG_FILE")

    """ This function is used to give the start date & end date for report pull """
    def get_date(self):
        self.start_date = datetime.now() - timedelta(31)
        self.end_date = datetime.now() - timedelta(1)
        return { "start_date":self.start_date,
                 "end_date" : self.end_date}


    """This  function will check for the file exist in s3 bucket"""
    def check_file_exist(self, file_path):
        key = file_path
        try:
            self.s3_client.head_object(Bucket=self.get_bucket_name(), Key=key)
        except ClientError:
            return False
        return True

    '''This function is used for executing the command using subprocess. This will raise exception in case of any and 
            stops the execution'''
    def execute_command(self, cmd):
        try:
            output = subprocess.check_output(
                cmd, stderr=subprocess.STDOUT, shell=True)
            self.write_to_log(cmd)
            self.write_to_log(output)
            return output
        except subprocess.CalledProcessError as exc:
            self.write_to_log("Status : FAIL", exc.output)
            raise Exception(exc)
        except Exception as e:
            self.write_to_log("Output: \n{}\n".format(e))
            raise Exception(e)
