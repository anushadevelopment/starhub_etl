import psycopg2
from psycopg2 import sql
from analysis import analysis
import logging


global conn
global cursor

logger = logging.getLogger(__name__)

def load_data(order_merge, active_services):
    res = create_table(order_merge, active_services)  
    
    if res == 'success':
        res = analysis(conn, cursor)
    return res
        
# Establishing a connection with Postgressql
def create_conn():
    host = "localhost"
    port = "5432"
    database = "starhub"
    user = "postgres"
    password = "root"
    global conn
    try:
        conn = psycopg2.connect(
            host = host,
            port = port,
            database = database,
            user = user,
            password = password
            
        )
        logger.info(f" Establishing Postgressql connection is successful")
    except Exception as e:
        logger.error(f"ERROR: Error while establishing a connection with Postgressql. Exception: {e}")
        raise e
    return conn

# Creating active_services and order_tb and service_analysis_snapshot tabke if not exists in Postgressql
def create_table(order_merge, active_services):
    try:
        global cursor
        conn = create_conn()
        cursor = conn.cursor()
        
        create_table_active = '''
        CREATE TABLE IF NOT EXISTS active_service(
            id SERIAL PRIMARY KEY,
            report_date DATE, 
            customer_id BIGINT,
            service_id BIGINT,
            service_name VARCHAR(255),
            subscription_status VARCHAR(50)
            
        );
        '''
        create_table_order = ''' 
        CREATE TABLE IF NOT EXISTS order_tb(
            id SERIAL PRIMARY KEY,
            service_id BIGINT,
            service_name VARCHAR(20),
            service VARCHAR(50),
            report_date DATE,
            order_type VARCHAR(50),
            order_type_l2 VARCHAR(50),
            customer_id BIGINT,
            customer_segment_flag VARCHAR(50),
            customer_gender CHAR(1),
            customer_nationality VARCHAR(30)
        
        );
        '''
        create_table_analysis=''' 
        CREATE TABLE IF NOT EXISTS service_analysis_snapshot (
            id SERIAL PRIMARY KEY,
            service_id BIGINT,
            service_name VARCHAR(255),
            customer_id BIGINT,
            customer_gender CHAR(1),
            customer_nationality VARCHAR(30),
            report_date DATE,
            order_type VARCHAR(50),
            order_type_l2 VARCHAR(50),
            customer_segment_flag VARCHAR(50),
            active_status VARCHAR(50),
            is_new_signup BOOLEAN,           -- Track if it's a new signup
            is_churn BOOLEAN,                -- Track if the service is churned
            is_transfer BOOLEAN,             -- Track if the signup is a transfer
            is_active BOOLEAN,	             -- Track if is_active or not active 
            current_status VARCHAR(50),      -- Current status of the service (Active/Inactive)
            aggregate_week DATE,             -- Week aggregation 
            aggregate_month DATE             -- Month aggregation 
        );
        '''
        cursor.execute(create_table_active)
        cursor.execute(create_table_order)
        cursor.execute(create_table_analysis)
        conn.commit()
        logger.info(f" Table creation is successful")
        
        res = insert_data(order_merge, active_services)
    except Exception as e:
        logger.error(f"ERROR: Error while creating a table. Exception: {e}")
        raise e
    
    return res
    

# Inserting the transformed records to active_services and order_tb
def insert_data(order_merge, active_services):
    insert_query_active = sql.SQL(''' 
    INSERT INTO active_service (
        report_date, 
        customer_id,
        service_id,
        service_name,
        subscription_status
        
    ) VALUES(%s, %s, %s, %s, %s)

    ''')
    try:                       
        for index, row in active_services.iterrows():
            cursor.execute(insert_query_active, (
                        row['REPORT_DATE'],
                        row['CUSTOMER_ID'],
                        row['SERVICE_ID'],
                        row['SERVICE_NAME'],
                        row['SUBSCRIPTION_STATUS']))
        logger.info("Record insertion of active_services table is successful")
    except Exception as e:
        logger.error(f"ERROR: Error while inserting records to active_services table. Exception: {e}")
        raise e
        
        
    insert_query_order = sql.SQL(
        ''' 
        INSERT INTO order_tb (
            service_id,
            service_name,
            service,
            report_date,
            order_type,
            order_type_l2,
            customer_id,
            customer_segment_flag,
            customer_gender,
            customer_nationality
        
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
    )

    try:
        for index, row in order_merge.iterrows():
            cursor.execute(insert_query_order , (
                row['SERVICE_ID'],
                row['SERVICE_NAME'],
                row['SERVICE'], 
                row['REPORT_DATE'],
                row['ORDER_TYPE'],
                row['ORDER_TYPE_L2'],
                row['CUSTOMER_ID'],
                row['CUSTOMER_SEGMENT_FLAG'],
                row['CUSTOMER_GENDER'],
                row['CUSTOMER_NATIONALITY']
            ))
        logger.info("Record insertion of order_tb table is successful")

    except Exception as e:
        logger.error(f"ERROR: Error while inserting records to order_merge table. Exception: {e}")
        raise e
        
    conn.commit()
    # cursor.close()
    # conn.close()
    
    return 'success'





