from airflow.models import DAG 
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from operators.Hydra import HydraOperator
import pendulum

from helpers.forecast.rap_cns import run_check, run_down, check_down
from helpers.common import dataset_check, task_success_alert

args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': pendulum.duration(minutes=1),
    "queue": "master",
    "target": "rap_cns",
}

with DAG(
    dag_id='rap_cns_00',
    default_args=args,
    schedule_interval='3 5 * * *',
    start_date=pendulum.today('UTC').add(days=-1),
    tags=['rap','download'],
    catchup=False,
    max_active_tasks=4,
) as dag:

    rod = "00"
    data_rod = "{{ next_ds_nodash }}"


    check = PythonOperator(
        task_id="ftp_check",
        python_callable=run_check,
        op_kwargs={"date_rod": data_rod, "rod": rod} ,
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
        op_kwargs={"date_rod": data_rod, "rod": rod, "model": "rap_cns"},
        on_success_callback=task_success_alert,
    )

    with TaskGroup("Hydra_Process", tooltip="Hydra_Process") as Hydra_Process:
        Hydra_msg2nc = HydraOperator(
            task_id="Hydra_msg2nc",
            process="msg2nc",
            date=f"{data_rod}{rod}",
        )
        Hydra_mergetime = HydraOperator(
            task_id="Hydra_mergetime",
            process="mergetime",
            date=f"{data_rod}{rod}",
        )

        with TaskGroup("Hydra_Calc", tooltip="Hydra_Calc") as Hydra_Calc:
            Hydra_calc_primary = HydraOperator(
                task_id="Hydra_calc",
                process="calc",
                date=f"{data_rod}{rod}",
            )
            Hydra_calc_primary

        Hydra_msg2nc >> Hydra_mergetime >> Hydra_Calc


    check >> down >> down_check >> Hydra_Process >> check_dataset