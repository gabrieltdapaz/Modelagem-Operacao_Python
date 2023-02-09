from datetime import datetime
import logging as log
import os

from helpers.common import check_ftp, download_ftp, task_falha


def run_check(date_rod, rod):

    date = datetime.strptime(date_rod + rod, "%Y%m%d%H")

    for hora in range(00, 22):
        file_in = f""
        check_ftp(file_in)
    return


def run_down(date_rod, rod):

    date = datetime.strptime(date_rod + rod, "%Y%m%d%H")

    path_out = f""
    os.makedirs(path_out, exist_ok=True)

    for hora in range(00, 22):
        file_in = f""
        file_out = f""

        if not os.path.isfile(file_out):
            download_ftp(file_in,file_out)
    return


def check_down(date_rod, rod):

    date = datetime.strptime(date_rod + rod, "%Y%m%d%H")
    path_out = f""

    for hora in range(00, 22):
        file_in = f""

        if not os.path.isfile(file_in):
            task_falha(f"")

        else:
            log.info(f"")
    return
