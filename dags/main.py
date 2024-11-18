from datetime import datetime
import csv
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

# from airflow.providers.amazon.aws.operators.s3 import S3UploadOperator

def read_and_process_address_csv():
    data = pd.read_csv('/usr/local/airflow/dags/address.csv')

    data['ADDRESS_ID'] = data['ADDRESS_ID'].apply(lambda x: int(float(x)) if pd.notnull(x) else None)

    processed_data = [line.strip() for line in data]

    data.to_csv('/usr/local/airflow/dags/processed_address_csv.csv', index=False)


def read_and_process_autoinsurance_churn_csv():
    # Read the CSV file
    with open('/usr/local/airflow/dags/autoinsurance_churn.csv', 'r') as f_read_and_process_autoinsurance_churn_csv:
        data = f_read_and_process_autoinsurance_churn_csv.readlines()

    # Process the data (if needed)
    # For example, you might filter, transform, or clean the data here
    processed_data = data

    # Write the processed data to a new CSV file
    with open('/usr/local/airflow/dags/processed_autoinsurance_churn_csv.csv', 'w') as f_processed_autoinsurance_churn_csv:
        # for line in processed_data:
        #     f_processed_autoinsurance_churn_csv.write(line + '\n')
        f_processed_autoinsurance_churn_csv.writelines(processed_data)

def read_and_process_customer_csv():
    # Read the CSV file
    with open('/usr/local/airflow/dags/customer.csv', 'r') as f_read_and_process_customer_csv:
        data = f_read_and_process_customer_csv.readlines()

    # Process the data (if needed)
    # For example, you might filter, transform, or clean the data here
    processed_data = data

    # Write the processed data to a new CSV file
    with open('/usr/local/airflow/dags/processed_customer_csv.csv', 'w') as f_processed_customer_csv:
        # for line in processed_data:
        #     f_processed_customer_csv.write(line + '\n')
        f_processed_customer_csv.writelines(processed_data)

def read_and_process_demographic_csv():
    # Read the CSV file
    with open('/usr/local/airflow/dags/demographic.csv', 'r') as f_read_and_process_demographic_csv:
        data = f_read_and_process_demographic_csv.readlines()

    # Process the data (if needed)
    # For example, you might filter, transform, or clean the data here
    processed_data = data

    # Write the processed data to a new CSV file
    with open('/usr/local/airflow/dags/processed_demographic_csv.csv', 'w') as f_processed_demographic_csv:
        # for line in processed_data:
        #     f_processed_demographic_csv.write(line + '\n')
        f_processed_demographic_csv.writelines(processed_data)


def read_and_process_termination_csv():
    # Read the CSV file
    with open('/usr/local/airflow/dags/termination.csv', 'r') as f_read_and_process_termination_csv:
        data = f_read_and_process_termination_csv.readlines()

    # Process the data (if needed)
    # For example, you might filter, transform, or clean the data here
    processed_data = data

    # Write the processed data to a new CSV file
    with open('/usr/local/airflow/dags/processed_termination_csv.csv', 'w') as f_processed_termination_csv:
        f_processed_termination_csv.writelines(processed_data)

        # for line in processed_data:
        #     f_processed_termination_csv.write(line + '\n')

def upload_address_to_s3_task():
    hook = S3Hook('aws_default')
    hook.load_file(
        filename='/usr/local/airflow/dags/processed_address_csv.csv',
        key='airflow-address.csv',
        bucket_name='auto-insurance-churn-data',
        replace=True
    )

def upload_autoinsurance_churn_to_s3_task():
    hook = S3Hook('aws_default')
    hook.load_file(
        filename='/usr/local/airflow/dags/processed_autoinsurance_churn_csv.csv',
        key='airflow-autoinsurance_churn.csv',
        bucket_name='auto-insurance-churn-data',
        replace=True
    )

def upload_customer_to_s3_task():
    hook = S3Hook('aws_default')
    hook.load_file(
        filename='/usr/local/airflow/dags/processed_customer_csv.csv',
        key='airflow-customer.csv',
        bucket_name='auto-insurance-churn-data',
        replace=True
    )

def upload_demographic_to_s3_task():
    hook = S3Hook('aws_default')
    hook.load_file(
        filename='/usr/local/airflow/dags/processed_demographic_csv.csv',
        key='airflow-demographic.csv',
        bucket_name='auto-insurance-churn-data',
        replace=True
    )

def upload_termination_to_s3_task():
    hook = S3Hook('aws_default')
    hook.load_file(
        filename='/usr/local/airflow/dags/processed_termination_csv.csv',
        key='airflow-termination.csv',
        bucket_name='auto-insurance-churn-data',
        replace=True
    )


with DAG(
    'csv_to_s3_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup("parallel_read_csv_tasks") as parallel_read_csv_tasks:

        read_and_process_address_csv = PythonOperator(
            task_id='read_and_process_address_csv',
            python_callable=read_and_process_address_csv
        )

        read_and_process_autoinsurance_churn_csv = PythonOperator(
            task_id='read_and_process_autoinsurance_churn_csv',
            python_callable=read_and_process_autoinsurance_churn_csv
        )

        read_and_process_customer_csv = PythonOperator(
            task_id='read_and_process_customer_csv',
            python_callable=read_and_process_customer_csv
        )

        read_and_process_demographic_csv = PythonOperator(
            task_id='read_and_process_demographic_csv',
            python_callable=read_and_process_demographic_csv
        )

        read_and_process_termination_csv = PythonOperator(
            task_id='read_and_process_termination_csv',
            python_callable=read_and_process_termination_csv
        )


    with TaskGroup("parallel_upload_to_s3_task") as parallel_upload_to_s3_task:

        upload_address_to_s3_task = PythonOperator(
            task_id='upload_address_to_s3_task',
            python_callable=upload_address_to_s3_task
        )

        upload_autoinsurance_churn_to_s3_task = PythonOperator(
            task_id='upload_autoinsurance_churn_to_s3_task',
            python_callable=upload_autoinsurance_churn_to_s3_task
        )

        upload_customer_to_s3_task = PythonOperator(
            task_id='upload_customer_to_s3_task',
            python_callable=upload_customer_to_s3_task
        )

        upload_demographic_to_s3_task = PythonOperator(
            task_id='upload_demographic_to_s3_task',
            python_callable=upload_demographic_to_s3_task
        )

        upload_termination_to_s3_task = PythonOperator(
            task_id='upload_termination_to_s3_task',
            python_callable=upload_termination_to_s3_task
        )



    # with TaskGroup("parallel_create_table") as parallel_create_table:

    create_address_table = PostgresOperator(
        task_id='create_address_table',
        postgres_conn_id='redshift_default',
        sql="""
-- Create address table
CREATE TABLE IF NOT EXISTS public.address (
ADDRESS_ID VARCHAR(20) PRIMARY KEY,
LATITUDE DECIMAL(10,8),
LONGITUDE DECIMAL(10,8),
STREET_ADDRESS VARCHAR(100),
CITY VARCHAR(50),
STATE CHAR(2),
COUNTY VARCHAR(50)
);

        """
    )

    create_customer_table = PostgresOperator(
        task_id='create_customer_table',
        postgres_conn_id='redshift_default',
        sql="""
-- Create customer table
CREATE TABLE IF NOT EXISTS public.customer (
INDIVIDUAL_ID VARCHAR(20) PRIMARY KEY,
ADDRESS_ID VARCHAR(20),
CURR_ANN_AMT DECIMAL(10,2),
DAYS_TENURE INT,
CUST_ORIG_DATE VARCHAR(20),
AGE_IN_YEARS DECIMAL(5,2),
DATE_OF_BIRTH VARCHAR(20),
SOCIAL_SECURITY_NUMBER VARCHAR(15),
FOREIGN KEY (ADDRESS_ID) REFERENCES address (ADDRESS_ID)
);
        """
    )

    create_demographic_table = PostgresOperator(
        task_id='create_demographic_table',
        postgres_conn_id='redshift_default',
        sql="""
-- Create demographic table
CREATE TABLE IF NOT EXISTS public.demographic (
INDIVIDUAL_ID VARCHAR(20) PRIMARY KEY,
INCOME DECIMAL(10,2),
HAS_CHILDREN BOOLEAN,
LENGTH_OF_RESIDENCE DECIMAL(10,2),
MARITAL_STATUS VARCHAR(10),
HOME_MARKET_VALUE VARCHAR(50),
HOME_OWNER BOOLEAN,
COLLEGE_DEGREE BOOLEAN,
GOOD_CREDIT BOOLEAN,
CONSTRAINT fk_customer FOREIGN KEY (INDIVIDUAL_ID) REFERENCES customer (INDIVIDUAL_ID)
);
        """
    )

    create_termination_table = PostgresOperator(
        task_id='create_termination_table',
        postgres_conn_id='redshift_default',
        sql="""
-- Create termination table
CREATE TABLE IF NOT EXISTS public.termination (
INDIVIDUAL_ID VARCHAR(20) PRIMARY KEY,
ACCT_SUSPD_DATE VARCHAR(20),
CONSTRAINT fk_customer_termination FOREIGN KEY (INDIVIDUAL_ID) REFERENCES customer (INDIVIDUAL_ID)
);
        """
    )

    create_autoinsurance_churn_table = PostgresOperator(
        task_id='create_autoinsurance_churn_table',
        postgres_conn_id='redshift_default',
        sql="""
-- Create autoinsurance_churn table
CREATE TABLE IF NOT EXISTS public.autoinsurance_churn (
INDIVIDUAL_ID VARCHAR(20),
ADDRESS_ID VARCHAR(20),
CURR_ANN_AMT DECIMAL(10,2),
DAYS_TENURE INT,
CUST_ORIG_DATE VARCHAR(20),
AGE_IN_YEARS DECIMAL(5,2),
DATE_OF_BIRTH VARCHAR(20),
LATITUDE DECIMAL(10,8),
LONGITUDE DECIMAL(10,8),
CITY VARCHAR(50),
STATE CHAR(2),
COUNTY VARCHAR(50),
INCOME DECIMAL(10,2),
HAS_CHILDREN BOOLEAN,
LENGTH_OF_RESIDENCE DECIMAL(10,2),
MARITAL_STATUS VARCHAR(10),
HOME_MARKET_VALUE VARCHAR(50),
HOME_OWNER BOOLEAN,
COLLEGE_DEGREE BOOLEAN,
GOOD_CREDIT BOOLEAN,
ACCT_SUSPD_DATE VARCHAR(20),
CHURN BOOLEAN,
CONSTRAINT fk_customer_autoinsurance FOREIGN KEY (INDIVIDUAL_ID) REFERENCES customer (INDIVIDUAL_ID),
CONSTRAINT fk_address_autoinsurance FOREIGN KEY (ADDRESS_ID) REFERENCES address (ADDRESS_ID)
);
        """
    )



    # with TaskGroup("parallel_copy_to_redshift") as parallel_copy_to_redshift:

    copy_address_data_to_redshift = PostgresOperator(
        task_id='copy_address_data_to_redshift',
        postgres_conn_id='redshift_default',
        sql="""
            copy public.address from 's3://auto-insurance-churn-data/airflow-address.csv' IAM_ROLE 'arn:aws:iam::448049825343:role/service-role/AmazonRedshift-CommandsAccessRole-20241112T022829' csv ignoreheader 1;
        """
    )

    copy_customer_data_to_redshift = PostgresOperator(
        task_id='copy_customer_data_to_redshift',
        postgres_conn_id='redshift_default',
        sql="""
            copy public.customer from 's3://auto-insurance-churn-data/airflow-customer.csv' IAM_ROLE 'arn:aws:iam::448049825343:role/service-role/AmazonRedshift-CommandsAccessRole-20241112T022829' csv ignoreheader 1;
        """
    )

    copy_demographic_data_to_redshift = PostgresOperator(
        task_id='copy_demographic_data_to_redshift',
        postgres_conn_id='redshift_default',
        sql="""
            copy public.demographic from 's3://auto-insurance-churn-data/airflow-demographic.csv' IAM_ROLE 'arn:aws:iam::448049825343:role/service-role/AmazonRedshift-CommandsAccessRole-20241112T022829' csv ignoreheader 1;
        """
    )

    copy_termination_data_to_redshift = PostgresOperator(
        task_id='copy_termination_data_to_redshift',
        postgres_conn_id='redshift_default',
        sql="""
            copy public.termination from 's3://auto-insurance-churn-data/airflow-termination.csv' IAM_ROLE 'arn:aws:iam::448049825343:role/service-role/AmazonRedshift-CommandsAccessRole-20241112T022829' csv ignoreheader 1;
        """
    )

    copy_autoinsurance_churn_data_to_redshift = PostgresOperator(
        task_id='copy_autoinsurance_churn_data_to_redshift',
        postgres_conn_id='redshift_default',
        sql="""
            copy public.autoinsurance_churn from 's3://auto-insurance-churn-data/airflow-autoinsurance_churn.csv' IAM_ROLE 'arn:aws:iam::448049825343:role/service-role/AmazonRedshift-CommandsAccessRole-20241112T022829' csv ignoreheader 1;
        """
    )

    # with TaskGroup("parallel_upload_to_s3_task") as parallel_upload_to_s3_task:

    #     load_to_redshift_task = PythonOperator(
    #         task_id='load_to_redshift_task',
    #         python_callable=load_to_redshift
    #     )


    end = DummyOperator(task_id='end')
    # [read_and_process_address_csv >> upload_address_to_s3_task >> create_address_table], [read_and_process_autoinsurance_churn_csv >> upload_autoinsurance_churn_to_s3_task >> create_autoinsurance_churn_table] >> end

    start >> parallel_read_csv_tasks >> parallel_upload_to_s3_task >> create_address_table >> create_customer_table >> create_demographic_table >> create_termination_table >> create_autoinsurance_churn_table >> copy_address_data_to_redshift >> copy_customer_data_to_redshift >> copy_demographic_data_to_redshift >> copy_termination_data_to_redshift >> copy_autoinsurance_churn_data_to_redshift >> end
    
    # [read_and_process_address_csv, read_and_process_autoinsurance_churn_csv] >> [upload_address_to_s3_task, upload_autoinsurance_churn_to_s3_task] >> [create_address_table, ] >> copy_address_data_to_redshift

    # read_and_process_address_csv >> upload_address_to_s3_task >> create_address_table >> copy_address_data_to_redshift
