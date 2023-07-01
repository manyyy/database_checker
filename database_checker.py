import os
import pandas as pd
import numpy as np
# import pyodbc
import json
# from openpyxl.styles.numbers import *
import datetime
from datetime import datetime as dt
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import shutil
import schedule
import traceback

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

def date_ms_sql(x):
    return dt.strftime(x, '%Y%m%d')

def run_all(
    connection_string: str,
    sql: str,
    parameters_index: list,
    metrics: list,
    metrics_agg_funcs: dict,
    parameters_columns: list = [],
    metrics_check: list = [],
    sql_executes: list = [],
    mode: int = 0,
    seconds_wait: int = 1,
    do_at: str = '09:00',
    do_after: int = 9,
    date_start: datetime.date = yesterday,
    date_end: datetime.date = today,
    check_empty: bool = True,
    koef_rows: float = 2.0,
    koef_values: float = 2.0,
    dict_last_name: str = '',
    log_file_name: str = 'log_file',
    json_file_name: str = 'json_file',
    error_file_name: str = 'errors',
    error_emails: list = [],
    to_emails: list = [],
    send_text_start: str = '',
    send_text_end: str = '',
    connection_smtp: dict = {}
):
    if not parameters_index:
        raise ValueError("Добавьте параметры!")
    if len(parameters_columns) > 1:
        error_string = f"Добавленных параметров: {len(parameters_columns)}." +\
        "Для разбивки необходим только 1 параметр."
        raise ValueError(error_string)
    if not metrics:
        raise ValueError("Добавьте показатели!")
    if metrics_agg_funcs and len(metrics_agg_funcs) != len(metrics):
        raise ValueError("Число функций агрегации не совпадает с числом показателей!")
    elif not metrics_agg_funcs:
        print("Так как не введены функции агрегации, будет применяться sum для агрегирования.")
        metrics_agg_funcs = {el: 'sum' for el in metrics}
    if not metrics_check:
        metrics_check = metrics
    metrics_check_agg_funcs = {key: value for key, value in metrics_agg_funcs if key in metrics_check}
    date_start = date_ms_sql(date_start)
    date_end = date_ms_sql(date_end)
    cnxn = pyodbc.connect(connection_string)
    if sql_executes:
        cursor = cnxn.cursor()
        for sql_string in sql_executes:
            cursor.execute(sql_string.format(date_start, date_end))
            cnxn.commit()

    data = pd.read_sql_query(sql, cnxn)
    cnxn.close()
    if parameters_columns:
        data = pd.pivot_table(
            data,
            index=parameters_index,
            columns=parameters_columns,
            values=metrics,
            aggfunc=metrics_agg_funcs,
            fill_value=0,
            dropna=False
        ).reset_index()
    else:
        data = data.groupby(
            parameters_index,
            as_index=False,
            dropna=False
        ).agg(metrics_agg_funcs)
        data['empty'] = data.apply(lambda x: any(pd.isna(x[el]) for el in metrics_check), axis='columns')
        all_rows = len(data)
        empty_rows = len(data[data['empty']])
