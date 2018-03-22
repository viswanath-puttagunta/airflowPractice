"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from pprint import pprint
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2018, 3, 1),  #If you trigger this in 2018, airflow will run every week since 2015-06-01 (tuesdays)
    'email': ['vish@divergence.ai'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('tutorialVish2', default_args=default_args, schedule_interval="0 10 * * TUE")

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

def my_hello_fn(ds, **kwargs):
    with open(kwargs['params']['bashFName'], 'w') as wf:   # file gets created in ~/airflow/dags/airflowPractice/
        wf.write("Task Running for Date: " + ds + "\n")    # wherever the dag python code is residing
    print("Task Running for Date: " + ds)
    return 0

def my_two_cents(ds, **kwargs):
    print('Hello How are you: ' + kwargs['params']['cent'])

#https://gist.github.com/flolas/076424abd8b012371b516da32807ca14
t2 = PythonOperator(
    task_id='python_kd',
    python_callable=my_hello_fn,
    params={'bashFName':'vish.sh'},
    provide_context=True,
    dag=dag)

#t3 = PythonOperator(
#    task_id='python_cent',
#    python_callable=my_two_cents,
#    params={'cent':'50'},
#    provide_context=True,
#    dag=dag)

t2.set_upstream(t1)
#t3.set_upstream(t2)
