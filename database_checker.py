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
    critical_rows_value: float = 1.0,
    critical_volume_value: float = 5.0,
    koef_rows: float = 2.0,
    koef_values: float = 2.0,
    dict_last_name: str = '',
    log_file_name: str = 'log_file',
    json_file_name: str = 'json_file',
    error_file_name: str = 'errors',
    print_messages: bool = True,
    error_emails: list = [],
    to_emails: list = [],
    critical_emails: list = [],
    subject: str = 'Проверка',
    send_text_start: str = '',
    send_text_end: str = '',
    connection_smtp: dict = {},
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
        data_empty = data[data['empty']]
        all_rows = len(data)
        empty_rows = len(data_send)
        value_list_all = [data[el].apply(metrics_agg_funcs[el]) for el in metrics_check]
        value_list_empty = [data_empty[el].apply(metrics_agg_funcs[el]) for el in metrics_check]
        part_val = 0
        part_val = max([value_list_empty / value_list_all for i in range(len(metrics_check))])

        empty_rows_percents = (empty_rows / all_shops) * 100
        # part_val = (problem_val / all_val) * 100
        critical_error = False
        if empty_rows_percents > critical_rows_value or part_val > critical_volume_value:
            critical_error = True
            alert_string = f'''
                <strong><span style="color: #e03e2d;">&#10060; Ошибка критическая! Требуется проверка!</span></strong><br><br>
            '''
            if print_messages:
                print('Ошибка критическая! Требуется проверка!')
        else:
            alert_string = f'''
                <strong><span style="color: #2f8241;">&#10003; Ошибка в пределах допустимого</span></strong><br><br>
            '''
            if print_messages:
                print('Ошибка в пределах допустимого!')
        alert_string += f'''
            Процент количества пустых магазинов: <b>{empty_rows_percents:9.1f} %</b> (граница: {critical_rows_value} %)<br>
            Максимальный среди проверяемых показателей процент оборота пустых магазинов: <b>{part_val:9.1f} %</b> (граница: {critical_volume_value} %)<br>
        '''
        if print_messages:
            print(f'''
                Процент количества пустых магазинов: {empty_rows_percents:9.1f} % (граница: {critical_rows_value} %)
                Максимальный среди проверяемых показателей процент оборота пустых магазинов: {part_val:9.1f} % (граница: {critical_volume_value} %)
            ''')
        if to_emails or critical_emails:
            if len(data_empty) > 0:
                main_critical_table = '''
                    <table class="table table_sort">
                        <thead>
                            <tr>
                                <th scope="col">№</th>
                '''
                for col in data_empty.columns:
                    main_critical_table += f'''<th scope="col">{col}</th>\n'''
                main_critical_table += '''
                            </tr>
                        </thead>
                        <tbody>
                '''
                main_index = 1
                for index, row in data_empty.iterrows():
                    main_critical_table += f'''
                        <tr>
                            <th scope="row">{main_index}</th>
                    '''
                    for col in data_empty.columns:
                        if isinstance(row[col], int):
                            main_critical_table += f'''<td>{row[col]:9.0f}</td>'''
                        elif isinstance(row[col], float):
                            main_critical_table += f'''<td>{row[col]:9.2f}</td>'''
                        else:
                            main_critical_table += f'''<td>{row[col]}</td>'''
                        main_critical_table += '''\n'''
                    main_critical_table += '''\n</tr>'''
                    main_index += 1
                main_critical_table += '''
                        </tbody>
                    </table>
                '''

                text = send_text_start + alert_string + main_critical_table + send_text_end
                s = smtplib.SMTP(connection_smtp['host'], connection_smtp['port'])
                s.login(connection_smtp['login'], connection_smtp['password'])
                from_email = connection_smtp['from']
                if critical_error:
                    to_emails = to_emails + critical_emails
                else:
                    to_emails = to_emails
                msg = MIMEMultipart('alternative')
                msg['Subject'] = subject
                msg['From'] = from_email
                msg['To'] = ','.join(to_emails)
                text_part = MIMEText(text, 'html')
                msg.attach(text_part)
                s.sendmail(from_email, to_email, msg.as_string())
                s.quit()
        else:
            main_critical_table = ''

