# Author: Anirudh P M
# Created Date: 2022-09-20
# Updated Date:
# Changes Done:

""" This module is used to pull the campaign report, campaign name, adgroup name & ad name from Reddit API.
As Reddit report API doesn't have the campaign name, adgroup name & ad name details, So we pull these details using other API link and finally merge to form
a single dataframe with all required fields """

import json
import sys
from io import BytesIO
from io import StringIO
import io
import psycopg2
from datetime import date,timedelta
import datetime
import pandas as pd
import time
import os
from config import global_config
from dotenv import load_dotenv
import psycopg2
import requests
import boto3
import numpy as np


class RedditReportPull:
    # This function is used for initialization
    def __init__(self):
        load_dotenv()
        self.global_config = global_config.GlobalConfig()
        self.client_config = None
        self.redshift_cred = self.global_config.get_redshift_credentials()
        # Connection to iSOCRATES redshift
        self.redshift_con = psycopg2.connect(dbname=self.redshift_cred['redshift_dbname'],
                                             host=self.redshift_cred['redshift_host'],
                                             port=self.redshift_cred['redshift_port'],
                                             user=self.redshift_cred['redshift_user'],
                                             password=self.redshift_cred['redshift_password'])
        self.redshift_cur = self.redshift_con.cursor()

        self.aws_access_key_id = self.get_aws_access_key_id()
        self.aws_secret_access_key = self.get_aws_secret_access_key()
        self.date_details= self.global_config.get_date()
        self.start_date= self.date_details['start_date']
        self.end_date= self.date_details['end_date']


    """ This function returns the access key id """

    def get_aws_access_key_id(self):
        return os.getenv('AWS_ACCESS_KEY_ID')

    """ This function returns the secret key """

    def get_aws_secret_access_key(self):
        return os.getenv('AWS_SECRET_ACCESS_KEY')

    """ This function is used to read the client config file from s3 """

    def read_client_config(self):
        try:
            self.global_config.write_to_log("Reading client config file")
            client_config_response = self.global_config.s3_resource.Object(self.global_config.get_bucket_name(),
                                                                           self.global_config.get_client_config_file())
            self.client_config = client_config_response.get()['Body'].read()
            self.client_config = json.loads(self.client_config)
        except Exception as e:
            print(e)
            self.global_config.write_to_log("Error while reading client config", "Error")
            self.global_config.write_to_log(e)
            self.global_config.write_to_log(e.args)
            raise e

    """ This function is to read the query json file from s3 """

    def read_query_file(self):
        try:
            self.global_config.write_to_log("Reading query file")
            query_config_response = self.global_config.s3_resource.Object(self.global_config.get_bucket_name(),
                                                                          self.global_config.get_query_config_file())
            self.query_string = query_config_response.get()['Body'].read()
            self.query_string = json.loads(self.query_string)
        except Exception as e:
            self.global_config.write_to_log("Error while reading query_config", "Error")
            self.global_config.write_to_log(e)
            self.global_config.write_to_log(e.args)
            raise e

    """ This function is used to generate access token"""
    def gen_access_token(self):
        try:
           client_auth = requests.auth.HTTPBasicAuth(os.getenv('auth_username'),os.getenv('auth_password'))
           post_data = {"grant_type": os.getenv("grant_type"), "username": os.getenv("api_username"), "password":os.getenv("api_password"), "scope":os.getenv("api_scope")}
           headers = {"User-Agent": os.getenv("User_Agent")}
           response = requests.post(os.getenv("access_token_url"), auth=client_auth, data=post_data, headers=headers)
           response.json()
           token = response.json()['access_token']
           return token
        except Exception as e:
            self.global_config.write_to_log("Error while generating access token", "Error")
            self.global_config.write_to_log(e)
            self.global_config.write_to_log(e.args)
            raise e


    """ This function is used to get the reports & metrics details from Reddit API and processed it to form a single data frame"""
    def get_response_data(self):
        try:

            reddit_header = {
                "Authorization": ("bearer " + self.access_token)
            }
            # Report data pull
            reddit_params_report = {
                'starts_at': self.start_date.strftime("%Y-%m-%d") + 'T07:00:00Z',
                'ends_at': self.end_date.strftime("%Y-%m-%d") + 'T07:00:00Z',
                'time_zone_id': 'GMT',
                'group_by': ['campaign_id', 'ad_group_id', 'ad_id', 'date']
            }
            campaign_request_report = requests.get(self.report_url.format(os.getenv('account_id')),
                headers=reddit_header,
                params=reddit_params_report)
            if len(campaign_request_report.json()['data'])==0:
                print("No report for today")
                self.global_config.write_to_log("No report data for today")
            else:
                self.df_report = pd.json_normalize(json.loads(campaign_request_report.content), record_path=['data'])
                self.global_config.write_to_log("Pulled the report data for today")

            # Ad Name data pull
            reddit_params_ads = {
                'starts_at': self.start_date.strftime("%Y-%m-%d") + 'T07:00:00Z',
                'ends_at': self.end_date.strftime("%Y-%m-%d") + 'T07:00:00Z',
                'time_zone_id': 'GMT',
                'group_by': ['ad_id', 'date']
            }
            campaign_request_ads = requests.get(self.ads_name_url.format(os.getenv('account_id')),
                headers=reddit_header,
                params=reddit_params_ads)
            if len(campaign_request_ads.json()['data'])==0:
                print("Failed to fetch list of ads by for an account ID")
                self.global_config.write_to_log("Failed to fetch list of ads by for an account ID")
            else:
                self.dtf_ads = pd.json_normalize(json.loads(campaign_request_ads.content), record_path=['data'])
                self.global_config.write_to_log("fetched list of ads by for an account ID")

            # Ad Group Name data pull
            reddit_params_ad_group = {
                'starts_at': self.start_date.strftime("%Y-%m-%d") + 'T07:00:00Z',
                'ends_at': self.end_date.strftime("%Y-%m-%d") + 'T07:00:00Z',
                'time_zone_id': 'GMT',
                'group_by': ['ad_group_id', 'date']
            }
            campaign_request_ad_group = requests.get(self.adgroup_name_url.format(os.getenv('account_id')),
                                                headers=reddit_header,
                                                params=reddit_params_ad_group)
            if len(campaign_request_ad_group.json()['data'])==0:
                print("Failed to fetch all ad groups belonging to an advertiser")
                self.global_config.write_to_log("Failed to fetch all ad groups belonging to an advertiser")
            else:
                self.dtf_ad_group = pd.json_normalize(json.loads(campaign_request_ad_group.content), record_path=['data'])
                self.global_config.write_to_log("fetched all ad groups belonging to an advertiser")

            # Campaign Name data pull
            reddit_params_campaign = {
                'starts_at': self.start_date.strftime("%Y-%m-%d") + 'T07:00:00Z',
                'ends_at': self.end_date.strftime("%Y-%m-%d") + 'T07:00:00Z',
                'time_zone_id': 'GMT',
                'group_by': ['campaign_id', 'date']
            }
            campaign_request_campaign = requests.get(self.campaign_name_url.format(os.getenv('account_id')),
                                                     headers=reddit_header,
                                                     params=reddit_params_campaign)
            if len(campaign_request_campaign.json()['data'])==0:
                print("Failed to fetch campaigns by user details")
                self.global_config.write_to_log("Failed to fetch campaigns by user details")
            else:
                self.dtf_campaign = pd.json_normalize(json.loads(campaign_request_campaign.content), record_path=['data'])
                self.global_config.write_to_log("fetched campaigns by user details")

            #  Merging three Data frame to get Final Data frame with All required fileds
            ads_adgroup = pd.merge(left=self.dtf_ads[["name", "campaign_id", "ad_group_id", 'id']],
                                   right=self.dtf_ad_group[['name', 'id']], left_on='ad_group_id', right_on='id', how='left',
                                   suffixes=('_ads', '_adgroup'))
            self.dim_df = pd.merge(left=ads_adgroup.drop('id_adgroup', axis=1), right=self.dtf_campaign[['name', 'id']],
                              left_on='campaign_id', right_on='id',
                              suffixes=('_ads', '_campaign'), how='left')

            self.df_final = self.df_report

            # Adding Campaign name to the data frame
            campaign_id_list=[]
            campaign_name_list=[]
            for id in self.dim_df['campaign_id']:
                campaign_id_list.append(id)
            for name in self.dim_df['name']:
                campaign_name_list.append(name)
            campaign_name_dict = dict(zip(campaign_id_list,campaign_name_list))
            final_campaign_name = []
            for id in self.df_report["campaign_id"]:
                for campaign_id, campaign_name in campaign_name_dict.items():
                    if id==campaign_id:
                        final_campaign_name.append(campaign_name_dict[id])
            self.df_final['campaign_name']=final_campaign_name
            self.df_report['campaign_name'] = final_campaign_name

            # Adding Ad group name to the data frame
            group_id_list=[]
            group_name_list=[]
            for id in self.dim_df['ad_group_id']:
                group_id_list.append(id)
            for name in self.dim_df['name_adgroup']:
                group_name_list.append(name)
            ad_group_dict = dict(zip(group_id_list, group_name_list))
            final_adgroup_list=[]
            for id in self.df_report["ad_group_id"]:
                for ad_group_id, ad_group_name in ad_group_dict.items():
                    if id==ad_group_id:
                        final_adgroup_list.append(ad_group_dict[id])
            self.df_final['ad_group_name']=final_adgroup_list
            self.df_report['ad_group_name'] = final_adgroup_list

            # Adding Ad name to the data frame
            ad_id_list=[]
            ad_name_list=[]
            for id in self.dim_df['id_ads']:
                ad_id_list.append(id)
            for name in self.dim_df['name_ads']:
                ad_name_list.append(name)
            ads_dict = dict(zip(ad_id_list, ad_name_list))
            final_ad_list=[]
            for id in self.df_report["ad_id"]:
                for ad_id, ad_name in ads_dict.items():
                    if id==ad_id:
                        final_ad_list.append(ads_dict[id])
            self.df_final['ad_name']=final_ad_list
            self.df_report['ad_name'] = final_ad_list

            # Generating day,month,year,datenum filed
            day = []
            month = []
            year = []
            datenum = []
            for dt in self.df_final['date']:
                split = dt.split('-')
                day.append(split[2])
                month.append(split[1])
                year.append(split[0])
                datenum.append(str(split[0]) + str(split[1]) + str(split[2]))

            self.df_final = self.df_final[self.final_columns]

            column_names =self.columns_rename

            self.df_final.rename(columns=column_names, inplace=True)
            self.actual_DF=self.df_final
            self.actual_DF['client_name'] = self.client_name
            self.actual_DF['datasource'] = self.datasource
            self.actual_DF['year'] = year
            self.actual_DF['month'] = month
            self.actual_DF['day'] = day
            self.actual_DF['datenum'] = datenum

            self.push_all_raw_data_to_s3(self.df_report,self.s3_raw_file_path)
            self.convert_df_to_csv_and_push_to_s3(self.actual_DF,self.s3_file_path)


        except Exception as e:
            self.global_config.write_to_log("Error while pulling response", "Error")
            self.global_config.write_to_log(e)
            self.global_config.write_to_log(e.args)
            raise e

    def push_all_raw_data_to_s3(self, df, s3_raw_path_template):
        """This method will push all the raw data to the corresponding day/month in s3 replacing if data exist"""
        df_columns_list = df.columns.values.tolist()
        if 'date' in df_columns_list:
            grouped = df.groupby('date')
            for day in df['date'].unique():
                print(day)
                date=str(day).split('-')
                print(date)
                s3_raw_path = self.get_raw_file_location(s3_raw_path_template, date[0],date[1],date[2])
                df_grouped = grouped.get_group(day)
                self.convert_df_to_csv_and_push_to_s3(df_grouped, s3_raw_path)

    def get_raw_file_location(self,s3_raw_path_template,year,month,day):
        """This function will replace year month and day to return the s3 key for raw file"""
        s3_raw_path = s3_raw_path_template.replace('<replace_year>', year)
        s3_raw_path = s3_raw_path.replace('<replace_month>', month)
        s3_raw_path = s3_raw_path.replace('<replace_day>', day)
        return s3_raw_path


    """ This function is to convert df to csv and push raw and actual reports separately to s3 """

    def convert_df_to_csv_and_push_to_s3(self, df, s3_path):
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf8')

            # reset stream position
            csv_buffer.seek(0)

            self.global_config.s3_client.put_object(Body=csv_buffer.getvalue(),
                                                    Bucket=self.global_config.get_bucket_name(),
                                                    Key=s3_path)

            self.global_config.write_to_log('Successfully pushed Reddit report to s3', 'info')
            csv_buffer.close()
        except Exception as e:
            self.global_config.write_to_log(e, 'Error')
            print(e)
            raise (e)

    """ This function is to insert/update the data to redshift table"""

    def push_fact_data_to_redshift(self):
        try:
            for dt in self.actual_DF['datenum'].unique():
                # To delete data if any in redshift for the given date
                query = "delete from {}.{} where datenum={}".format(os.getenv('REDSHIFT_SCHEMA'),
                                                                    os.getenv('REDSHIFT_TABLE'),dt)
                print(query)
                self.redshift_cur.execute(query)
                self.redshift_con.commit()

            # insert data to redshift table
            query = "copy {}.{} from 's3://{}/{}' credentials 'aws_access_key_id={};aws_secret_access_key={}' IGNOREHEADER 1 " \
                     "csv;".format(os.getenv('REDSHIFT_SCHEMA'),
                                     os.getenv('REDSHIFT_TABLE'),
                                     self.global_config.get_bucket_name(),
                                     self.s3_file_path,
                                     self.get_aws_access_key_id(),
                                     self.get_aws_secret_access_key())
            print(query)
            self.redshift_cur.execute(query)
            self.redshift_con.commit()
        except psycopg2.ProgrammingError as e:
            print("Error while performing delete_copy on table - {}".format(os.getenv('REDSHIFT_TABLE')))
            print(e)
            self.redshift_con.rollback()
            self.redshift_cur.close()
            self.redshift_con.close()
            raise e

    """ This function is to delete last 30th day file from s3"""

    def delete_s3_file(self):
        try:
            date_30_ago = datetime.datetime.now() - timedelta(days=31)
            s3 = boto3.client('s3',
                              aws_access_key_id=self.get_aws_access_key_id(),
                              aws_secret_access_key=self.get_aws_secret_access_key()
                              )

            response = s3.list_objects_v2(Bucket=self.global_config.get_bucket_name())['Contents']
            for data in response:
                if 'tiktok_data_' + date_30_ago.strftime('%Y-%m-%d') in data['Key']:
                    delete_filename = data['Key']
                    self.global_config.s3_client.delete_object(Bucket=os.getenv('BUCKET_NAME'), Key=delete_filename)
        except Exception as e:
            self.global_config.write_to_log("Error while deleting the file from s3", "Error")
            self.global_config.write_to_log(e)
            self.global_config.write_to_log(e.args)
            print(e)
            raise e


def main():
    reddit_obj = RedditReportPull()
    reddit_obj.read_client_config()
    reddit_obj.read_query_file()
    for id in reddit_obj.client_config:
        reddit_obj.client_id = id
        reddit_obj.global_config.write_to_log("Client - {}".format(id))
        client_config = reddit_obj.client_config[id]
        for client_data in client_config:
             if client_data['execute'] != 'True':
                 continue
             reddit_obj.datasource_id = client_data['datasource_id']
             reddit_obj.client_name = client_data['client_name']
             reddit_obj.datasource = client_data['datasource']
        for query_client_id in reddit_obj.query_string:
            query_config = reddit_obj.query_string[query_client_id]
            for query_data in query_config:
                reddit_obj.s3_file_path=query_data['s3_file_path']
                reddit_obj.s3_raw_file_path=query_data['s3_raw_file_path']
                reddit_obj.redshift_table_name=query_data['redshift_table_name']
                reddit_obj.report_url=query_data['report_url']
                reddit_obj.campaign_name_url=query_data['campaign_name_url']
                reddit_obj.adgroup_name_url=query_data['adgroup_name_url']
                reddit_obj.ads_name_url=query_data['ads_name_url']
                reddit_obj.final_columns=query_data['final_columns']
                reddit_obj.columns_rename=query_data['columns_rename']
            reddit_obj.access_token=reddit_obj.gen_access_token()
            reddit_obj.get_response_data()
            #reddit_obj.convert_df_to_csv_and_push_to_s3()
            reddit_obj.push_fact_data_to_redshift()

    #reddit_obj.delete_s3_file()
    reddit_obj.redshift_cur.close()
    reddit_obj.redshift_con.close()

if __name__ == '__main__':
    main()

