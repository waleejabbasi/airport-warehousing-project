from airflow.sdk import dag,task
import sys
sys.path.insert(0,"/opt/airflow/airport-snowflake-elt")
from scripts.load import run_pipeline

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
        dag_id='airport_dag',

)
def airport_dag():
    @task.python
    def start_etl():
        run_pipeline()

    etl_task=start_etl()

    staging_task=SQLExecuteQueryOperator(
        task_id='load_staging',
        sql='load_staging1.sql',
        conn_id="snowflake_conn"
    )

   
    cust_dim_task=SQLExecuteQueryOperator(
        task_id='cust_dimension',
        sql='customer_dimension2.sql',
        conn_id="snowflake_conn"
    )

    airport_dim_task=SQLExecuteQueryOperator(
        task_id='airport_dimension',
        sql='airport_dimension3.sql',
        conn_id="snowflake_conn"
    )
    pilot_dim_task=SQLExecuteQueryOperator(
        task_id='pilot_dimension',
        sql='pilot_dimension3.sql',
        conn_id="snowflake_conn"
    )

    date_dim_task=SQLExecuteQueryOperator(
        task_id='date_dimension',
        sql='date_dimension.sql',
        conn_id="snowflake_conn"
    )

    loading_dim_task=SQLExecuteQueryOperator(
        task_id='loading_fact',
        sql='loading_fact.sql',
        conn_id="snowflake_conn"
    )

    etl_task >> staging_task >> [cust_dim_task,airport_dim_task,pilot_dim_task,date_dim_task] >> loading_dim_task

airport_dag()