from airflow.models import DAG 
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from operators.Hydra import HydraOperator
import pendulum
from datetime import datetime, timedelta 

from helpers.forecast.silam_glo import run_check, run_down, check_down
from helpers.common import dataset_check, task_success_alert

args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': pendulum.duration(minutes=1),
    "queue": "master",
    "target": "silam_glo",
}

with DAG(
    dag_id='silam_glo_00',
    default_args=args,
    schedule_interval='3 18 * * *',
    start_date=pendulum.today('UTC').add(days=-1),
    tags=['Chemical model'],
    catchup=False,
    max_active_tasks=4,
) as dag:

    rod = "00"
    data_rod = ''

    check = PythonOperator(
        task_id="ftp_check",
        python_callable=run_check,
        op_kwargs={"date_rod": data_rod, "rod": rod},
        retries=20,
        retry_delay=timedelta(seconds=60),
    )
    down = PythonOperator(
        task_id="download",
        python_callable=run_down,
        op_kwargs={"date_rod": data_rod, "rod": rod} ,
    )
    down_check = PythonOperator(
        task_id="down_check",
        python_callable=check_down,
        op_kwargs={"date_rod": data_rod, "rod": rod} ,
    )
    check_dataset = PythonOperator(
        task_id="check_dataset",
        python_callable=dataset_check,
        op_kwargs={"date_rod": data_rod, "rod": rod, "model": args['target']},
        on_success_callback=task_success_alert,
    )

    with TaskGroup("Hydra_Process", tooltip="Hydra_Process") as Hydra_Process:
        Hydra_msg2nc = HydraOperator(
            task_id="Hydra_msg2nc",
            process="split_var",
            date=f"{data_rod}{rod}",
        )
        Hydra_msg2nc

    check >> down >> down_check >> Hydra_Process >> check_dataset
