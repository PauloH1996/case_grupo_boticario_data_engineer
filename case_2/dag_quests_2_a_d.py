from datetime import datetime, timedelta

from numpy import string_
from proto import STRING


from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


default_args = {
    'owner': 'Paulo Alcantara',
    'depends_on_past': False,
    'start_date': datetime(year=2023, month=3, day=12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    #'execution_timeout': timedelta(seconds=900),

}

with DAG(
    dag_id='case_gb_2_a_d',
    default_args=default_args,
    schedule_interval=None,
    catchup=False, #começar a executar de datas passadas
    description='Estudo de caso do grupo boticário (engenheiro de dados) - Case 2 número 2 da letra A até D ',
    tags=['criar tabelas'],
    template_searchpath='/home/airflow/gcs/dags/case_gb/consultas_sql/'
) as dag:


    start = DummyOperator(task_id='start')


    case_2_table_1 = BigQueryExecuteQueryOperator(
    task_id='caso_2_numero_2_letra_A',
    sql='case_2_table_1.sql',
    use_legacy_sql=False,
    gcp_conn_id="google_cloud_di_conn",
    dag=dag,
    )

    case_2_table_2 = BigQueryExecuteQueryOperator(
    task_id='caso_2_numero_2_letra_B',
    sql='case_2_table_2.sql',
    use_legacy_sql=False,
    gcp_conn_id="google_cloud_di_conn",
    dag=dag,
    )

    case_2_table_3 = BigQueryExecuteQueryOperator(
    task_id='caso_2_numero_2_letra_C',
    sql='case_2_table_3.sql',
    use_legacy_sql=False,
    gcp_conn_id="google_cloud_di_conn",
    dag=dag,
    )

    case_2_table_4 = BigQueryExecuteQueryOperator(
    task_id='caso_2_numero_2_letra_D',
    sql='case_2_table_4.sql',
    use_legacy_sql=False,
    gcp_conn_id="google_cloud_di_conn",
    dag=dag,
    )
 
    end = DummyOperator(task_id='end')

    
    start >> case_2_table_1 >> case_2_table_2 >> case_2_table_3 >> case_2_table_4 >> end 