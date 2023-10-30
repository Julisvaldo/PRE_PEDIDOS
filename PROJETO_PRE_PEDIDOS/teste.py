from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import datetime as dt
import PROJETO_AGRUPAMENTO

with DAG(
    "teste",
    start_date=dt.datetime(2023, 10, 16, 11, 0, 0),
    catchup=False,
    schedule="@daily",
) as dag :
        do_stuff1 = PythonOperator(
        task_id="task_1",
        python_callable=PROJETO_AGRUPAMENTO.executar_projeto,  
    )
