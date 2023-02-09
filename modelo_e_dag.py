
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from operators.Hydra import HydraOperator
from urllib.request import urlretrieve, urlopen
from urllib.error import HTTPError, URLError
import urllib
import pendulum
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
import os


args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': pendulum.duration(minutes=1),
    "queue": "master",
    "target": "silam_glo",
}   


def check_ftp(file_in):

   try:
        urllib.request.urlopen(file_in,timeout=100000)
   except HTTPError as e:
        print(f"Check_ftp(): Sem arquivo no ftp, {e.code}. {file_in}")
   except URLError as er:
        print("Check_ftp():except file_inError")
        check_ftp(file_in)
   else:
        print(f"Check_ftp(): Arquivo existe no ftp: {file_in}")
   return


def download_ftp(file_in, file_out):
   
    try:
        urllib.request.urlretrieve(file_in, file_out)
    except HTTPError as e:
        print(f"download_ftp(): Sem arquivo no ftp, {e.code}. {file_in}")
    else:
        print(f"download_ftp(): Arquivo baixado: {file_out}")
    return True



def run_down(date_rod):
 
    date = datetime.strptime(date_rod, '%Y%m%d')

    path_out = f""
    os.makedirs(path_out, exist_ok=True)

    file_in  = f''
    file_out = f'{path_out}/allergy_risk_{date.strftime("%Y%m%d")}.nc4'

    if not os.path.isfile(file_out):
        download_ftp(file_in,file_out)



def run_check(date_rod):
 
    date = datetime.strptime(date_rod, "%Y%m%d")

    file_in = f'https://silam.fmi.fi/thredds/fileServer/silam_allergy_risk/files/allergy_risk_{date.strftime("%Y%m%d")}.nc4'
    check_ftp(file_in)



with DAG(
    dag_id='silam_allergy_risk',
    default_args=args,
    schedule_interval='0 15 * * *',
    start_date=pendulum.today('UTC').add(days=-1),
    tags=['silam_allergy','download'],
    catchup=False,
    max_active_tasks=4,
) as dag:

    data_rod = "{{ ds_nodash }}"

    check = PythonOperator(
        task_id="ftp_check",
        python_callable=run_check,
        op_kwargs={"date_rod": data_rod} ,
    )

    down = PythonOperator(
        task_id="download",
        python_callable=run_down,
        op_kwargs={"date_rod": data_rod} ,
    )
    with TaskGroup("Hydra_Process", tooltip="Hydra_Process") as Hydra_Process:
        Hydra_nc4 = HydraOperator(
            task_id="Hydra_nc4",
            process='nc4',
            date=f"{data_rod}",
        )
        Hydra_mergetime = HydraOperator(
            task_id="Hydra_mergetime",
            process="mergetime",
            date=f"{data_rod}",
        )

        with TaskGroup("Hydra_Calc", tooltip="Hydra_Calc") as Hydra_Calc:
            Hydra_calc_primary = HydraOperator(
                task_id="Hydra_calc",
                process="calc",
                date=f"{data_rod}",
            )
            Hydra_calc_primary
        
        Hydra_nc4 >> Hydra_mergetime >> Hydra_Calc
    
    check >> down >> Hydra_Process
