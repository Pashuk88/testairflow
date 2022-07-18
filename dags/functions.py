import datetime as dt
from dateutil.relativedelta import relativedelta as rd
from datetime import datetime
from numpy import set_string_function
import pymssql
from clickhouse_driver import Client
import calendar
import pandas as pd
import pendulum
from datetime import timedelta, datetime
from airflow.hooks.base import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook

#from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
import logging
#from cassandra.cluster import Cluster
#from cassandra.policies import RoundRobinPolicy

def default_args():
    local_tz = pendulum.timezone("UTC")
    default_args = {
        'start_date': datetime(2021, 1, 1, tzinfo=local_tz),
        'owner': 'airflow',
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        #'end_date': datetime(2021, 5, 16, 23, 54)}
    }
    return default_args


def ch_query(connection_id):
    '''
    Функция, в которой задаются параметры подключения к ClickHouse и выполняется запрос query.
    Ничего не возвращает, просто выполняет запрос
    Аргументы:
    - 'query: String' - SQL-запрос, который необходимо выполнить 
    '''
    connection = BaseHook.get_connection(connection_id)

    client = Client(
        host=connection.host,
        port=connection.port,
        database=connection.schema,
        user=connection.login,
        password=connection.password)
    #for i in range(1):
        #query = client.execute(query)
    return client

# Функция, в которой задаются параметры подключения к MS SQL Server и выполняется запрос query
# Возвращает запрос
def ms_con(connection_id):
    '''
    Функция, в которой задаются параметры подключения к MS SQL Server и выполняется запрос query. 
    Возвращает соединение с сервером conn
    '''
    connection = BaseHook.get_connection(connection_id)
    conn = pymssql.connect(server=connection.host, user=connection.login, password=connection.password, database=connection.schema, autocommit=True)
    return conn

# Функция, в которой задаются параметры подключения к PostgeSQL и выполняется запрос query
# Возвращает запрос
def postgresql_con(connection_id):
    '''
    Функция, в которой задаются параметры подключения к PostgeSQL и выполняется запрос query. 
    Возвращает соединение с сервером conn
    '''
    postgres_hook = PostgresHook(connection_id)
    conn = postgres_hook.get_sqlalchemy_engine()
    return conn

# Функция, в которой задаются параметры подключения к PostgeSQL и выполняется запрос query
# Возвращает запрос
def mysql_con(connection_id):
    '''
    Функция, в которой задаются параметры подключения к PostgeSQL и выполняется запрос query. 
    Возвращает соединение с сервером conn
    '''
    mysql_hook = MySqlHook(connection_id)
    conn = mysql_hook.get_sqlalchemy_engine()
    return conn

# def cassandra_con(connection_id, query):

#     cassandra_hook = CassandraHook(cassandra_conn_id=connection_id)

#     contact_points = list(vars(next(iter(vars(cassandra_hook).items()))[1]).items())[1][1]
#     keyspace = list(vars(cassandra_hook).items())[1][1]
#     # logging.warning(keyspace)
#     # logging.warning(type(keyspace))

#     cluster = Cluster(contact_points=contact_points)
#     session = cluster.connect(keyspace)
#     result = session.execute(query)
#     session.shutdown()
#     return result

# Функция для получения списка дат за n последних месяца по дням
#  Пример: если n=2, то [20210401, 20210402,..., 20210531]; если n=3, то [20210301, 20210302,..., 20210531]
def get_n_months_days(n):
    '''
    Функция для получения списка дат за n последних месяца по дням
    Аргументы:
    - 'n: Int' - Количество месяцев
    Возвращает:
    - Список с датами на каждый день за n месяцев
    Пример:
    print(get_n_months_days(2))
    [20210401, 20210402,..., 20210531]

    print(get_n_months_days(3))
    [20210301, 20210302,..., 20210531]
    '''
    year = datetime.now().year
    months = dict()
    for i in range(n):
        month = datetime.now().month - (n-i)
        months[month] = calendar.monthrange(year, month)[1]
    list = []
    for key in months.keys():
        for j in range(months[key]):
            if key <= 9 and j <=8:
                list.append(str(year)+'0'+str(key)+'0'+str(j+1))
            elif key > 9 and j <=8:
                list.append(str(year)+str(key)+'0'+str(j+1))
            elif key <= 9 and j > 8:
                list.append(str(year)+'0'+str(key)+str(j+1))
    return list


def get_two_dates(n, to=1, time=False, sep='-'):
    '''
    Функция, которая возвращает список с двумя датами: начало (текущий месяц - n) и конец прошлого месяца
    Аргументы:
    - 'n: Int' - Количество месяцев (назад)
    - 'to: Int' - до какого месяца (назад)
    - 'time: Logical' - добавление времени к дате при True, без времени при False
    - 'sep: String' - разделитель между yyyy.mm.dd По умолчанию -
    Возвращает:
    - список из двух дат
    Пример
    get_two_dates(2, True, '.')
    ['2021.04.01 00:00:00', '2021.05.31 23:59:59']
    '''
    if time == False:
        current_year = datetime.now().year
        month1 = datetime.now().month - to
        month2 = datetime.now().month - n
        m = calendar.monthrange(current_year, month1)[1]
        if month1 <= 9:
            month1 = '0'+str(month1) 
        if month2 <= 9:
            month2 = '0'+str(month2) 
        two_months_ago = str(current_year)+sep+str(month2)+sep+'01'
        one_month_ago = str(current_year)+sep+str(month1)+sep+str(m)
        return [two_months_ago, one_month_ago]
    else:
        current_year = datetime.now().year
        month1 = datetime.now().month - to
        month2 = datetime.now().month - n
        m = calendar.monthrange(current_year, month1)[1]
        if month1 <= 9:
            month1 = '0'+str(month1) 
        if month2 <= 9:
            month2 = '0'+str(month2) 
        two_months_ago = str(current_year)+sep+str(month2)+sep+'01 00:00:00'
        one_month_ago = str(current_year)+sep+str(month1)+sep+str(m)+' 23:59:59'
        return [two_months_ago, one_month_ago]


def execute_comm(query, connection_id):
    """Функция обращения к Хранилищу DNS на чтение
    Параметры:
    query - строка, SQL запрос
    Возвращает - pandas DataFrame с содержанием результата выполнения запроса, названия столбцов из запроса"""
    connection = BaseHook.get_connection(connection_id)
    conn = pymssql.connect(server=connection.host, user=connection.login, password=connection.password, database=connection.schema, autocommit=True)
    df = pd.read_sql(query, conn)
    return df

def crud(query, connection_id):
    connection = BaseHook.get_connection(connection_id)
    conn = pymssql.connect(server=connection.host, user=connection.login, password=connection.password, database=connection.schema, autocommit=True)
    try:
        with conn.cursor() as cursor:
            sql = query
            cursor.execute(sql)
        conn.commit()
    finally:
        conn.close()

def one_month(period_ago=2, sep='.'):
    """Функция для автоматической генерации дат для процедур ForSales и ForPayment
    Параметры:
    sep - разделитель
    Возвращает - список с двумя датами и временем: начало и конец позапрошлого месяца
    Если сегодня июль, то функция вернёт ['01.05.2021 00:00:00', '30.05.2021 23:59:59']"""
    current_year = datetime.now().year
    month = datetime.now().month - period_ago
    m = calendar.monthrange(current_year, month)[1]
    if month <= 9:
            month = '0'+str(month) 
    months_ago_start = '01'+sep+str(month)+sep+str(current_year)+' 00:00:00'
    month_ago_end = str(m)+sep+str(month)+sep+str(current_year)+' 23:59:59'
    return [months_ago_start, month_ago_end]

def last_month(sep='.'):
    """Функция для автоматической генерации дат для процедур ForSales и ForPayment
    Параметры:
    sep - разделитель
    Возвращает - список с двумя датами и временем: начало и конец позапрошлого месяца
    Если сегодня июль, то функция вернёт ['01.05.2021 00:00:00', '30.05.2021 23:59:59']"""
    current_year = datetime.now().year
    month = datetime.now().month - 1
    m = calendar.monthrange(current_year, month)[1]
    if month <= 9:
            month = '0'+str(month) 
    months_ago_start = str(current_year)+sep+str(month)+sep+'01'
    month_ago_end = str(current_year)+sep+str(month)+sep+str(m)
    return [months_ago_start, month_ago_end]


# Функция для получения списка дат за n последних месяца по месяцам
def get_n_months(start_date=datetime(2019, 1, 1)):
    '''
    Функция для получения списка дат за n последних месяцев
    Аргументы:
    - 'start_date: DateTime' - Начало списка
    Возвращает:
    - Список с датами на каждый день за n месяцев до прошлого месяца
    Пример:
    print(get_n_months())
    [2019-01-01, 2019-02-01,..., 2021-05-01]

    print(get_n_months_days(start_date=datetime(2020, 4, 1)))
    [2020-04-01, 2020-05-01,..., 2021-05-01]
    '''
    now = datetime(datetime.now().year, datetime.now().month - 1, 1)
    start_date = start_date
    months_list = [start_date.strftime('%Y-%m-%d')]

    while start_date <= now:
        start_date += timedelta(days=31)
        months_list.append( datetime(start_date.year, start_date.month, 1).strftime('%Y-%m-%d') )
    return months_list


def get_n_days():
    '''
    Функция для получения списка дат за n последних месяцев
    Аргументы:
    - 'start_date: DateTime' - Начало списка
    Возвращает:
    - Список с датами на каждый день за n месяцев до прошлого месяца
    Пример:
    print(get_n_months())
    [2019-01-01, 2019-02-01,..., 2021-05-01]

    print(get_n_months_days(start_date=datetime(2020, 4, 1)))
    [2020-04-01, 2020-05-01,..., 2021-05-01]
    '''
    now = datetime(2021, 10, 30)
    start_date = datetime(2019, 8, 1)
    months_list = [start_date.strftime('%Y-%m-%d')]

    while start_date <= now:
        start_date += timedelta(days=1)
        months_list.append( datetime(start_date.year, start_date.month, start_date.day).strftime('%Y-%m-%d') )
    return months_list



def dates_list(serial='m', date_end=False, cnt=1, span=False, sep='-', wm=True):
    '''
    Функция для получения списка дат
    Аргументы:
    - 'serial: String' - тип возвращаемых дат (m - месяц, d - день)
    - 'date_end: Date' - дата, до которой необходимо вернуть данные (если не указан, возвращает вчерашнюю дату)
    - 'cnt: Int' - кол-во месяцев назад
    - 'span: Logical' - возвращение диапазона дат (True - вернуть все даты начала месяца при serial = 'm'/вернуть все даты по дням при serial = 'd';
                                                    False - вернуть дату начала и конца диапазона)
    - 'sep: String' - разделитель
    - 'wm: Logical' - возвращение закрытого месяца (True - вернуть все даты до начала текущего месяца;
                                                    False - вернуть даты по вчерашний день)
    Возвращает:
    - Список с датами
    
    Пример:
    print(dates_list())
    ['2021-12-01', '2021-12-31']

    print(dates_list(wm=False))
    ['2022-01-01', '2022-01-09']

    print(dates_list(date_end='2021-12-01'))
    ['2021-11-01', '2021-11-30']

    print(dates_list(date_end='2021-12-01', cnt=3))
    ['2021-09-01', '2021-11-30']

    print(dates_list(date_end='2021-12-01', cnt=3, span=True))
    ['2021-09-01', '2021-10-01', '2021-11-01']

    print(dates_list(serial='d', date_end='2021-12-01', cnt=3, span=True))
    ['2021-09-01', '2021-09-02', ..., '2021-11-30']
    '''

    format =  f'%Y{sep}%m{sep}%d'
    list = []    
    if date_end:
        try:
            end_date =  dt.date.fromisoformat(date_end) - dt.timedelta(days=1)
        except:
            end_date = dt.date.today() - dt.timedelta(days=1)
        start_date = end_date.replace(day=1) - rd(months=cnt-1)
    else:
        if wm:
            end_date = dt.date.today().replace(day=1) - dt.timedelta(days=1)
        else:
            end_date = dt.date.today() - dt.timedelta(days=1)
        start_date = end_date.replace(day=1) - rd(months=cnt-1)
    if span:
        if serial == 'm':
            while start_date <= end_date:
                list.append(start_date.strftime(format))
                start_date += rd(months=1)
        else:
            while start_date <= end_date:                
                list.append(start_date.strftime(format))
                start_date += dt.timedelta(days=1)
    else:
        list.append(start_date.strftime(format))
        list.append(end_date.strftime(format))
    return list