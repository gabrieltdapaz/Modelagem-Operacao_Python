from helpers.common import check_ftp, download_ftp, task_falha
from datetime import datetime, timedelta 
import logging as log
import os
from multiprocessing.dummy import Pool


url = 'https://silam.fmi.fi/thredds/ncss/silam_glob_v5_7_1/files'

def run_check(date_rod, rod):

    date = datetime.strptime(date_rod + rod, "%Y%m%d%H")
    deltatime = datetime.strptime(date_rod + rod, "%Y%m%d%H")

    list_ftp = []
    for time in range(1, 169):

        deltatime += timedelta(hours=1)

        file_in = f"{url}/SILAM-AQ-glob_v5_7_1_{date.strftime('%Y%m%d')}{rod}_{time:03}.nc4?var=CO_column&var=NO2_column&var=O3_column&var=SO2_column&var=soa_column&var=sslt_column&var=temp_2m&disableLLSubset=on&disableProjSubset=on&horizStride=1&time_start={deltatime.strftime('%Y-%m-%dT%H')}%3A00%3A00Z&time_end={deltatime.strftime('%Y-%m-%dT%H')}%3A00%3A00Z&timeStride=1&vertCoord=&addLatLon=true&accept=netcdf4"
        list_ftp.append([file_in])


    with Pool(processes=30) as pool:
        pool.starmap(check_ftp, list_ftp)

    return


def run_down(date_rod, rod):

    date = datetime.strptime(date_rod + rod, "%Y%m%d%H")
    deltatime = datetime.strptime(date_rod + rod, "%Y%m%d%H")

    path_out = f"/data/forecast/silam_glo/RAW/{date.strftime('%Y/%j/%H')}"
    os.makedirs(path_out, exist_ok=True)

    list_ftp = []
    for time in range(1, 169):

        deltatime += timedelta(hours=1)

        file_in = f"{url}/SILAM-AQ-glob_v5_7_1_{date.strftime('%Y%m%d')}{rod}_{time:03}.nc4?var=CO_column&var=NO2_column&var=O3_column&var=SO2_column&var=soa_column&var=sslt_column&var=temp_2m&disableLLSubset=on&disableProjSubset=on&horizStride=1&time_start={deltatime.strftime('%Y-%m-%dT%H')}%3A00%3A00Z&time_end={deltatime.strftime('%Y-%m-%dT%H')}%3A00%3A00Z&timeStride=1&vertCoord=&addLatLon=true&accept=netcdf4"
        file_out = f"{path_out}/SILAM-AQ-glob_v5_7_1_{date.strftime('%Y%m%d')}{rod}_{time:03}.nc4"

        if not os.path.isfile(file_out):
            list_ftp.append([file_in, file_out])


    log.warning(f"Down(): lista de: {len(list_ftp)}")
    log.warning(f"Down(): {list_ftp}")

    with Pool(processes=30) as pool:
        pool.starmap(download_ftp, list_ftp)

    return



def check_down(date_rod, rod):

    date = datetime.strptime(date_rod + rod, "%Y%m%d%H")
    deltatime = datetime.strptime(date_rod + rod, "%Y%m%d%H")
    path_out = f"/data/forecast/silam_glo/RAW/{date.strftime('%Y/%j/%H')}"

    for time in range(1, 169):

        deltatime += timedelta(hours=1)

        file_out = f"{path_out}/SILAM-AQ-glob_v5_7_1_{date.strftime('%Y%m%d')}{rod}_{time:03}.nc4"

        if not os.path.isfile(file_out):
            task_falha(f"Check_down(): Arquivo ainda não está em: {file_out}")

        else:
            log.info(f"Check_down(): Arquivo já existe em: {file_out}")
    return
