
# 2. Process the Excel File:
#    - Read the 4 files using Python’s pandas’ library.
#      - Orders: Combine and clean the data from the 3 files (Service, Customer, Order). Take only those that can be mapped with Service file.
#      - Actives: Process the data from the active file to capture the list of active services as of the previous day.
#    - Perform basic data cleaning, including handling missing values, adjusting column data types, and any necessary formatting to prepare the data for ingestion.

import pandas as pd
from pathlib import Path
import os
from datetime import datetime
import logging
import confuse
import argparse
global config

import extract
from load_data import load_data



def transform(active_df,customer_df,orders_df,service_df):
    
    # unique service_id
    unique_service_id = service_df['SERVICE_ID'].unique()
    
    # filter orders mapped with service
    orders_df_mapped_service = orders_df[orders_df['SERVICE_ID'].isin(unique_service_id)]
    # print (orders_df.head(10))

    # convert active_df columns to appropriate data types
    active_df = active_df.astype({
    'SERVICE_NAME' : 'string',
    'SUBSCRIPTION_STATUS' : 'string'
    })
    active_df['REPORT_DATE'] =  pd.to_datetime(active_df['REPORT_DATE'], format='%d/%m/%Y')

    # convert customer_df columns to appropriate data types
    customer_df = customer_df.astype({
        'CUSTOMER_SEGMENT_FLAG': 'string',
        'CUSTOMER_GENDER': 'category',
        'CUSTOMER_NATIONALITY': 'string'
    })
    customer_df['REPORT_DATE'] = pd.to_datetime(customer_df['REPORT_DATE'], format='%Y-%m-%d')

    # convert service_df columns to appropriate data types
    service_df[['SERVICE_NAME','SERVICE']] = service_df[['SERVICE_NAME','SERVICE']].astype('string')
    service_df['REPORT_DATE'] = pd.to_datetime(service_df['REPORT_DATE'], format='%Y-%m-%d')

    #clean customer data
    customer_df['CUSTOMER_NATIONALITY'] = customer_df['CUSTOMER_NATIONALITY'].fillna('Unknown')
    customer_df['CUSTOMER_GENDER'] = customer_df['CUSTOMER_GENDER'].fillna('u')
    customer_df.dropna(subset=['CUSTOMER_ID'], inplace=True)

    # clean active
    active_df['SUBSCRIPTION_STATUS'] = active_df['SUBSCRIPTION_STATUS'].fillna('Unknown')


    #Merge Service, Customer, Order - mapped with Service

    merge_orders_service = pd.merge(service_df[['SERVICE_ID', 'SERVICE_NAME', 'SERVICE']], orders_df, on='SERVICE_ID',how='inner' )
    customer_has_service_id = pd.merge(customer_df, active_df[['SERVICE_ID','CUSTOMER_ID']], on='CUSTOMER_ID', how='inner')
    order_merge = pd.merge(merge_orders_service[['SERVICE_ID', 'SERVICE_NAME', 'SERVICE', 'ORDER_TYPE',
        'ORDER_TYPE_L2']], customer_has_service_id, on=['SERVICE_ID'], how="inner")


    #- Actives: Process the data from the active file to capture the list of active services as of the previous day.
    # Get the previous day date
    previous_day = datetime.now() - pd.Timedelta(days=1)

    # Filter active services as of the previous day 
    active_services = active_df[(active_df['REPORT_DATE'] == previous_day)]

    # create necessary tables and load data into tables
    res = load_data(order_merge, active_services)
   
               


if __name__ == '__main__':
    
    # Directory from which the files will be extracted
    input_dir = Path.cwd() / "main/Data"

    # Logging 
    logging_dir = Path.cwd() /"main/logs"
    logging_dir.mkdir(parents=True, exist_ok=True)
    
    config = confuse.Configuration('main')
    parser = argparse.ArgumentParser(description='starhub_etl')
    parser.add_argument("-l", "--log", dest="logLevel", choices=['DEBUG','INFO','WARNING','ERROR'])
    args = parser.parse_args()
    config.set_args(args)
    
    
    script_name = 'starhub_etl'
    # logging_location = logging_dir + script_name + datetime.now().strftime(".%Y%m%d")+ '.log' 
    logging_location = os.path.join(logging_dir, script_name + datetime.now().strftime(".%Y%m%d") + '.log')
    print(logging_location)
    
    logging.basicConfig(
        filename=logging_location,
        level = 'INFO',
        format = '%(asctime)s,%(msecs)03d %(levelname)s %(module)s:%(lineno)d - %(funcName)s: %(message)s', 
        datefmt="%Y-%m-%d %H:%M:%S", )
    
    
    logging.info("Script running")
        

    # Read all the four csv files from the Input directory
    run = False
    try:
        active_df = pd.read_csv(os.path.join(input_dir, 'Raw Active.csv'))
        customer_df = pd.read_csv(os.path.join(input_dir, 'Raw Customer.csv'))
        orders_df = pd.read_csv(os.path.join(input_dir, 'Raw Orders.csv'))
        service_df = pd.read_csv(os.path.join(input_dir, 'Raw Service.csv'))
        run = True
    except Exception as e:
        logging.error(f"ERROR: Error while extracting files from {input_dir}. Exception: {e}")
        raise e

if run:
    try:
        logging.info("ETL started")
        transform(active_df,customer_df,orders_df,service_df)
        logging.info("ETL completed successfully! ")
        
    except Exception as e:
        print("Error in starhub_etl", e)
        logging.error("Error in starhub_etl", e)
else:
    exit(0)