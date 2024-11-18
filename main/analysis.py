#    - Flattened table will be a snapshot of orders and status of the service on a daily, weekly and monthly frequency
#    - This table should be able to track:
#      - Whether a service is a new signup.
#      - Whether a service has made a churn event.
#      - Whether the new signup is a transfer
#      - The current status of the service (e.g., active or inactive)

import psycopg2
from psycopg2 import sql
import logging

logger = logging.getLogger(__name__)

# Join and aggregate the data from the order table and the active service table to create a flattened table that will be used for analysis.
def analysis(conn, cursor):
    try:
        insert_query_analysis = sql.SQL( '''
        WITH order_service_CTE AS(

        SELECT
                o.service_id,
                o.service_name,
                o.customer_id,
                o.customer_gender,
                o.customer_nationality,
                o.report_date,
                o.order_type,
                o.order_type_l2,
                o.customer_segment_flag,
                a.subscription_status as active_status,
        CASE
            WHEN o.order_type_l2 = 'new' THEN true
            ELSE false 	
        END AS is_new_signup,
        CASE 
            WHEN o.order_type_l2 = 'terminate' THEN true
            ELSE false
        END AS is_churn,
        CASE  
            WHEN o.order_type_l2 = 'transfer' THEN true
            ELSE false
        END AS is_transfer,
        CASE 
            WHEN a.subscription_status='active' THEN true
        ELSE false
        END AS is_active,
        date_trunc('week', o.report_date) as aggregate_week,
        date_trunc('month', o.report_date) as aggregate_month
        
        FROM order_tb o
        
        LEFT JOIN  
            active_service a
        ON
            o.service_id = a.service_id AND o.customer_id  = a.customer_id 
        ) 
        INSERT into service_analysis_snapshot(
                service_id,
                service_name,
                customer_id,
                customer_gender,
                customer_nationality,
                report_date,
                order_type,
                order_type_l2,
                customer_segment_flag,
                active_status,
                is_new_signup,
                is_churn,
                is_transfer,
                is_active,
                aggregate_week,
                aggregate_month
        )
        SELECT
                service_id,
                service_name,
                customer_id,
                customer_gender,
                customer_nationality,
                report_date,
                order_type,
                order_type_l2,
                customer_segment_flag,
                active_status,
                is_new_signup,
                is_churn,
                is_transfer,
                is_active,
                aggregate_week,
                aggregate_month
        FROM order_service_CTE ;
        '''
        )
        cursor.execute(insert_query_analysis)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Record insertion of service_analysis_snapshot table is successful")
    except Exception as e:
        logger.error(f"ERROR: Error while inserting record to service_analysis_snapshot table. Exception: {e}")
        raise e
    return 'success'