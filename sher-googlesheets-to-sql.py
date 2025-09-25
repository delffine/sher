import os
import re
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import psycopg2
import warnings
warnings.filterwarnings("ignore")


# ------------------------------------------ 
#       Вспомогательные процедуры
# ------------------------------------------


# инициализация соединения с базой
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except psycopg2.OperationalError as e:
        print(f"The error {e} occurred")
    return connection


# запрос на создание базы
def create_database(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        logg("Query executed successfully")
    except psycopg2.OperationalError as e:
        logg(f"The error occurred: {e}")
    return


# запрос с ответом "ОК / не ОК"
# p=False - вообще без ответа
def execute_query(connection, query, p=True):
    try:
        connection.cursor()
    except:
        init_connection()
        base_reset()   
        
    connection.autocommit = True
    cursor = connection.cursor()    
    try:
        cursor.execute(query)
        if p: logg(f'Query executed successfully {query[0:20]}...')
        result = True    
    except psycopg2.OperationalError as e:
        logg(f"The error occurred: {e}")
        result = False
    return result



# запрос с выводом данных в табличной форме
def execute_read_query(connection, query):
    try:
        connection.cursor()
    except:
        init_connection()
        base_reset()    

    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) > 0:
            dd = pd.DataFrame(np.array(result))
            dd.columns = [desc[0] for desc in cursor.description]
        else:
            dd = pd.DataFrame([])
        return dd
    except psycopg2.OperationalError as e:
        logg(f"The error occurred: {e}")
    return 

# лекарство от зависших ссессий
def base_reset():
    connection.rollback()
    connection.autocommit = True
    return


def create_sql_table(name, data, prkey):
    """
    Процедура создания новой таблицы в SQL
    На входе имя таблицы, датафрейм и название колонки первичного ключа. 
    Пустая строка '', если надо создать таблицу без ключа 
    Команда создание табилцы формируется из названия колонок датафрейма и типа данных.
    В случает существования таблицы команда игнорируется.
    """
    cols = dict(data.dtypes)
    sql_st = """CREATE TABLE
    IF NOT EXISTS """ + name + """ (
        """
    for c in cols:
        dtyp = cols[c]
        if dtyp == 'int64': cols[c] = 'INTEGER'
        if dtyp == 'float64': cols[c] = 'NUMERIC'        
        if dtyp == 'datetime64[ns]': cols[c] = 'TIMESTAMP'
        if dtyp == 'object': cols[c] = 'VARCHAR'
        if dtyp == 'bool': cols[c] = 'VARCHAR'            
        if c == prkey: cols[c] = 'SERIAL PRIMARY KEY'
        sql_st = sql_st + c + " " + cols[c] + """, """
    sql_st = sql_st[:-2]
    sql_st = sql_st +  ")"
    execute_query(connection, sql_st)
    return

def export_to_sql(table, data, p=True):
    """
    Процедура вставки новых строк в SQL
    На входе имя таблицы и датафрейм
    Команда на добавление формируется из названия колонок датафрейма и его данных.
    Один запрос включает максимум 1000 команд.
    В случает конфликта команда на добавление строчки - игнорируется.
    """
    inrows = 1000
    for l in range(len(data) // inrows + 1):
        insert_row = """INSERT INTO """ + table  + """ (""" + ', '.join(data.columns) + """ ) VALUES"""
        cols = data.columns
        tt = data.iloc[l*inrows:l*inrows + inrows]
        for i in range(len(tt)):
            istr = '('
            for c in cols:
                if tt[c].dtypes != 'int' and tt[c].dtypes != 'float64':                    
                    nstr = str(tt.iloc[i][c]).replace("'", "")
                    if nstr == 'nan' or nstr == 'NaN' or nstr == 'None':
                        istr += "NULL, "
                    else:
                        istr += f"'{nstr}', "                   
                else:
                    nstr = str(tt.iloc[i][c])
                    if nstr == 'nan' or nstr == 'NaN' or nstr == 'None':
                        istr += "NULL, "
                    else:
                        istr += nstr + ', '
            istr = istr[:-2]
            istr += '), '+"\r\n"
            insert_row += istr
        insert_row = insert_row[:-4]
        insert_row += ' ON CONFLICT DO NOTHING ;'
        execute_query(connection, insert_row, False)
    if p:
        logg(f'Экспортировано в sql базу строк {l*inrows + len(tt)}', 3)
    return


def generate_md5_hash(input_string):
    """
    Процедура расчета хэша входной строки по алгоритму md5
    Используется в таблицах, где нет одной уникальной колонки
    """
    md5_hash = hashlib.md5()
    md5_hash.update(input_string.encode('utf-8'))
    return md5_hash.hexdigest()

def init_connection():
    """
    Процедура инициализации соединения с SQL базой
    Настройки соединения берутся из файла
    Из этого же файла берутся ключи аутентификации API
    """
    global connection
    global dash_list_id 
    global log_lev
    
    path = ''
    db_set = dict()
    file = open(path + "db_connect.txt")
    for line in file:
        k, v = line.strip().split('=')
        db_set[k] = v
    file.close()
    connection = create_connection(db_set['dbname'], db_set['uname'], db_set['pass'], db_set['server'], db_set['port'])

    dash_list_id = db_set['dash_list_id']
    log_lev = int(db_set['log_level'])

    db_set = dict()
    return


def init_googlesheets():
    global spread_sheets
    path = ''
    credentials_path = path + 'sher-googlesheets.json'
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)
        service = build('sheets', 'v4', credentials = creds)
        spread_sheets = service.spreadsheets()
        print('Connection to Spread_sheets successful')
    except Exception as e:
        print(f'Ошибка! {e}')    

    return


def logg(st='', lev=0):
    if lev <= log_lev: print(f'{st}')
    sql = f"INSERT INTO log (date, script, mess, log_lev) VALUES ('{datetime.now()}', 'gs', '{st}', {lev})"
    execute_query(connection, sql, False)
    return

def garant_float(x):
    try:
        res = float(x)
    except:
        res = None
    return res   

# ------------------------------------------ 
#              ELT скрипты
# ------------------------------------------
def elt_gs_dash(gs_data):
    """
    Преобразование однотипных листов GS с дашбордами для загурзки в SQL
    Колонка A 'status' - определяет, будет ли браться строчка в SQL
    Колонка B 'data' - опрелеляется либо прямо из B, 
        либо берется из последней даты в колонке D   
    Колонка C 'name' - опрелеляется либо прямо из С, 
        либо берется из последнего имени в колонки D   
    Колонка D 'par' - опрелеляется прямо из D   
    Колонка H 'var' - преобразуется в float
    В итоге берутся только колонки A, B, C, D, H
    Строчки берутя только тем, где в B - дата, C - не дата, D - не дата
    Если колонка А статус пустое, то ставиться 1, то есть беруться все строчки по умолчаюнию
    """    
    name = ''
    month = ''
    mass = []
    
    months = {'Январь' : '01', 
          'Февраль' : '02', 
          'Март' : '03', 
          'Апрель' : '04', 
          'Май' : '05',
          'Июнь' : '06',
          'Июль' : '07',
          'Август' : '08',
          'Сентябрь' : '09',
          'Октябрь' : '10',
          'Ноябрь' : '11',
          'Декабрь' : '12'
         }
    
    gs_data.columns = [chr(i) for i in range(ord('A'), ord('A') + len(gs_data.columns))]
    for i in range(len(gs_data)):
        row = gs_data.iloc[i]
        if re.match(r"(\w+) (\d+)", str(row['D'])): month = row['D']   
        if str(row['H']).strip(' ').lower() == 'итого'\
            or str(row['E']).strip(' ').lower() == 'еи'\
            or str(row['G']).strip(' ').lower() == 'среднее': 
                name = row['D']
        if name == '': name = '! Не указано !'        
        if str(row['B']).strip(' ') == '':  row.loc['B'] = month
        if str(row['C']).strip(' ') == '':  row.loc['C'] = name

        row['H'] = str(row['H']).strip(' ').replace('р.', '').replace('р', '').replace(',', '.')
        row['H'] = re.sub(r'[^0-9.-]+', '', row['H'])  

        try:
            float(row['H'])
        except ValueError:
            True    
        else:
            mass.append(row)
    
    result = pd.DataFrame(mass).reset_index(drop=True)   
    result.columns = ('status', 'date', 'user_name', 'par', 'ed', 'plan', 'mean', 'var', 'day1')
    
    #заменяем пустые значения статуса на 1. те строчки с пустым статусом пройдут дальше 
    result.loc[:,'status'] = result['status'].replace('', '1').fillna('1')
    
    #берем только строчки с датами в датах и НЕ берем строчки с датами в именях, параметрах и имя "вакантно" 
    result = result.query('status != "0"\
        and date.str.lower().str.match(r"январь|февраль|март|апрель|май|июнь|июль|август|сентябрь|октябрь|ноябрь|декабрь (20\\d\\d)")\
        and ~user_name.str.lower().str.match(r"январь|февраль|март|апрель|май|июнь|июль|август|сентябрь|октябрь|ноябрь|декабрь (20\\d\\d)")\
        and ~par.str.lower().str.match(r"январь|февраль|март|апрель|май|июнь|июль|август|сентябрь|октябрь|ноябрь|декабрь (20\\d\\d)")\
        and ~user_name.str.lower().str.contains("вакантно")')[['status', 'date', 'user_name', 'par', 'plan', 'var']]

    try:
        result.loc[:,'plan'] = result.loc[:,'plan'].replace('-', '0')
        result.loc[:,'plan'] = result.loc[:,'plan'].str.strip(' ').str.replace('р.', '').str.replace('р', '').str.replace(',', '.')
        result['plan'] = result['plan'].apply(lambda x: garant_float(x))
    except Exception as e:
        logg(f'!!! Ошибка при переводе plan в float: {e}')

    try:
        result.loc[:,'var'] = result['var'].replace('', '0').replace('-', '0').fillna(0)
        result.loc[:,'var'] = result['var'].str.replace(',', '.').astype('float')
    except Exception as e:
        logg(f'!!! Ошибка при переводе var в float: {e}')

    try:
        result.loc[:,'date'] = result['date'].str.title().apply(lambda x: x.split(' ')[1] + '-' + months[x.split(' ')[0]] + '-01')
        result.loc[:,'date'] = result['date'].str.replace('г', '').str.replace('Г', '')
        result.loc[:,'date'] = pd.to_datetime(result['date']).dt.date
    except Exception as e:
        logg(f'!!! Ошибка при переводе в date: {e}')

    return result


def elt_gs_load_dash():
    """
    Загрузка данных из дашбордов GoogleSheets
    Список дашбордов берется из эксельки на гуглдиске 1m7WjgKgSsMp-m4epKj-CoB3ZCfIYEHaVAzB9s88KDms
    Затем в цикле обрабатываются все эксельки и складываются в один датасет
    К датасетам перед этим добаляется название дашбордов, откуда беруться данные.
    Данные экспоритруется в SQL, при этом сначала удаляются данные по успешно загруженным дашбордам
    То есть таблица НЕ чиститься TRUNC, чтобы сохранить в SQL данные, которые по какойто причине не загрузились из GS
    """
    logg('------  Начинаю экспорт данных из GoogleSheets по сотрудникам ------', 2)    
    dashbords_list = pd.DataFrame([])
    sheet_read = spread_sheets.values().get(spreadsheetId = dash_list_id, range = 'datalens-dashboards!A:D').execute()
    dashbords_list = pd.DataFrame(sheet_read['values'])[1:].reset_index(drop=True)
    dashbords_list.columns = ['dash', 'spread_sheet_url', 'sheet_list', 'type']
    dashbords_list = dashbords_list.query('type == "сотрудники"').reset_index(drop=True)

    logg(f'--- Всего из GS надо загрузить {len(dashbords_list)} дашбордов', 3)   
    all_dashboards = pd.DataFrame([])
    for i in range(len(dashbords_list)):
        dash = dashbords_list.loc[i]['dash']
        spread_sheet_id = dashbords_list.loc[i]['spread_sheet_url'].split('/')[5]
        spread_sheet_list = dashbords_list.loc[i]['sheet_list']
        logg(f"Загружаю {dash}: sh_id = {spread_sheet_id}, sh_list = {spread_sheet_list} ", 3)
        try:
            ggg = spread_sheets.values().get(spreadsheetId=spread_sheet_id, range=spread_sheet_list+'!A:I').execute()
            gs_data = pd.DataFrame(ggg['values'])
            pd_data = elt_gs_dash(gs_data)
            pd_data['dashboard'] = dash        
        except Exception as e:
            logg(f'!!! При обработке {dash} ошибка: {e}')
        if len(pd_data) > 0:
            all_dashboards = pd.concat([all_dashboards, pd_data])
        else:
            logg(f"!!! {dash} после обработки пустой")
    all_dashboards = all_dashboards.reset_index(drop=True)
    logg('--- ВСЕ, что смог, загрузил из GS! ---', 3)   
    if len(all_dashboards) > 0:
        logg(f"--- В SQL надо экспортировать {len(all_dashboards)} строк из дашбордов GS", 3)

        #удаляем из SQL не все, а только те дашборды, которые были успешно загружены из GS
        dash_delete = "'" + "'"", '".join(all_dashboards['dashboard'].unique()) + "'"
        execute_query(connection, f"""DELETE FROM gs_dashboards WHERE dashboard IN ({dash_delete})""", p = False)        
        
        export_to_sql('gs_dashboards', all_dashboards)
    else:
        logg('!!! Нет GS дашбордов для загрузки в SQL')
    logg('------  Экспорт данных из GoogleSheets по сотрудникам завершен ------', 3)    
    return

def etl_gs_stat_dash(gs_data):
    """
    Преобразование однотипных листов GS с дашбордами статистики для загурзки в SQL
    Сначала переименовываются колонки, так как в исходнике '№' и два 'Год'
    Название финансовых показателей проставляется в каждую строку
    Применятся операция melt - обратная созданию сводной таблицы pivot.
    Получается таблица всего из 4 колонок - год, параметр, месяц (куда попадают и кварталы) и значение показателя
    Проводится чистка параметра и показателя, для дальнейшего перевода в нужный тип. 
    """
    gs_data.columns = ['NN', 'Год', 'Финансовые показатели',
                  'Январь', 'Февраль', 'Март', 'Квартал 1',
                  'Апрель', 'Май', 'Июнь', 'Квартал 2',
                  'Июль', 'Август', 'Сентябрь', 'Квартал 3',
                  'Октябрь', 'Ноябрь', 'Декабрь', 'Квартал 4', 'За год']
    gs_data = gs_data[1:]
    gs_data = gs_data.drop(index = gs_data[gs_data['Январь'] == 'Январь'].index)
    gs_data = gs_data.drop('NN', axis = 1).reset_index(drop = True)

    for i in range(len(gs_data)):
        if gs_data.loc[i, 'Финансовые показатели'] == '' : 
            gs_data.loc[i, 'Финансовые показатели'] = par_last
        else:
            par_last = gs_data.loc[i, 'Финансовые показатели']
    gs_data = gs_data.dropna(subset=['Финансовые показатели'])

    #операция melt - обратная созданию сводной таблицы pivot
    gs_data = gs_data.melt(['Год', 'Финансовые показатели'], value_name='val').reset_index(drop=True)
    gs_data.columns = ['year', 'par', 'month', 'var']
    gs_data['var'] = gs_data['var'].str.replace('р.', '')\
                  .str.replace('\xa0', '')\
                  .str.replace('%', '')\
                  .str.replace(',', '.')\
                  .str.replace('', '')\
                  .str.replace('#DIV/0!', '')\
            .replace('-', '')\
            .replace('', '0').fillna('0')
    gs_data['par'] = gs_data['par'].str.replace('\n', ' ')\
                .str.replace('\r', ' ')\
                .str.replace('\t', ' ')   
    return gs_data

def elt_gs_load_stats_dash():
    """
    Загрузка дашбордов со статистикой
    Берутся данные в колонках с A по T
    Проводиться преобразовние процедурой etl_gs_stat_dash(). 
    На выходе таблица из 4 колонок - год, параметр, месяц и значение показателя
    Затем беруться только данные по месяцам
    Прописывается дата начала месяца, отдельно прописывается год, показатель var приводится к float
    Данные экспоритруется в SQL, при этом сначала удаляются данные по успешно загруженным дашбордам
    То есть таблица НЕ чиститься TRUNC, чтобы сохранить в SQL данные, которые по какойто причине не загрузились из GS 
    """
    logg('------  Начинаю экспорт данных из GoogleSheets по статитике ------', 2)
    sheet_read = spread_sheets.values().get(spreadsheetId = dash_list_id, range = 'datalens-dashboards!A:D').execute()
    dashbords_list = pd.DataFrame(sheet_read['values'])[1:].reset_index(drop=True)
    dashbords_list.columns = ['dash', 'spread_sheet_url', 'sheet_list', 'type']
    dashbords_list = dashbords_list.query('type == "статистика"').reset_index(drop=True)

    months = {'Январь' : '01', 
          'Февраль' : '02', 
          'Март' : '03', 
          'Апрель' : '04', 
          'Май' : '05',
          'Июнь' : '06',
          'Июль' : '07',
          'Август' : '08',
          'Сентябрь' : '09',
          'Октябрь' : '10',
          'Ноябрь' : '11',
          'Декабрь' : '12'
         }

    logg(f'--- Всего из GS надо загрузить {len(dashbords_list)} дашбордов по статистике ---', 3)   
    all_dashboards = pd.DataFrame([])
    pd_data = pd.DataFrame([])
    for i in range(len(dashbords_list)):
        dash = dashbords_list.loc[i]['dash']
        spread_sheet_id = dashbords_list.loc[i]['spread_sheet_url'].split('/')[5]
        spread_sheet_list = dashbords_list.loc[i]['sheet_list']    
        logg(f"Загружаю дашборд {dash}: sh_id = {spread_sheet_id}, sh_list = {spread_sheet_list} ", 3)
        try:
            ggg = spread_sheets.values().get(spreadsheetId=spread_sheet_id, range=spread_sheet_list+'!A:T').execute()
            gs_data = pd.DataFrame(ggg['values'])
            pd_data = etl_gs_stat_dash(gs_data)
            pd_data['dashboard'] = dash       
        except Exception as e:
            logg(f'!!! При обработке {dash} ошибка: {e}')

        if len(pd_data) > 0:
            all_dashboards = pd.concat([all_dashboards, pd_data])
        else:
            logg(f'!!! Дашборд {dash} после обработки пустой')

    all_dashboards = all_dashboards.query('month.isin(@months)').reset_index(drop=True)
    all_dashboards.loc[:,'date'] = all_dashboards.loc[:,'year'] + '-' + all_dashboards.loc[:,'month'].replace(months) + '-01'
    all_dashboards.loc[:,'date'] = all_dashboards.loc[:,'date'].str.replace('г', '').str.replace('Г', '')    
    all_dashboards.loc[:,'date'] = pd.to_datetime(all_dashboards.loc[:,'date']).dt.date    
    all_dashboards.loc[:,'var'] = all_dashboards.loc[:,'var'].astype('float')
    all_dashboards.loc[:,'year'] = all_dashboards.loc[:,'year'].astype('int')    
    all_dashboards = all_dashboards.sort_values(by='date').reset_index(drop=True)
    logg('--- ВСЕ, что смог, загрузил! ---', 3)
    if len(all_dashboards) > 0:
        logg(f'В SQL надо загрузить {len(all_dashboards)} строк из дашбордов GS по статистике', 3)

        #удаляем из SQL не все, а только те дашборды, которые были успешно загружены из GS
        dash_delete = "'" + "'"", '".join(all_dashboards['dashboard'].unique()) + "'"
        execute_query(connection, f"""DELETE FROM gs_stat_dashboards WHERE dashboard IN ({dash_delete})""", p = False)

        export_to_sql('gs_stat_dashboards', all_dashboards)
    else:
        logg('!!! Нет GS дашбордов для загрузки в SQL')
    logg('------  Экспорт данных из GoogleSheets по статитике завершен ------', 2)
    return


def elt_gs_costprice_wb():
    """
    Загрузка себестоимости товаров WB
    Из GS загружаются колонки с A по O, но в итоге остаются только 
    'Артикул WB', 'Себестоимость', 'Категория', 'Артикул продавца',
    Проходит переименование колонок в латинские названия 
    Данные эспортируются в SQL, предварительно полностью очистив таблицу
    """
    logg('------  Начинаю экспорт данных из GoogleSheets по себестоимости WB ------', 2)   

    dashbords_list = pd.DataFrame([])
    sheet_read = spread_sheets.values().get(spreadsheetId = dash_list_id, range = 'datalens-dashboards!A:D').execute()
    dashbords_list = pd.DataFrame(sheet_read['values'])[1:].reset_index(drop=True)
    dashbords_list.columns = ['dash', 'spread_sheet_url', 'sheet_list', 'type']
    dashbords_list = dashbords_list.query('type == "себестоимость WB"').reset_index(drop=True)
    dash = dashbords_list.loc[0]['dash']
    spread_sheet_id = dashbords_list.loc[0]['spread_sheet_url'].split('/')[5]
    spread_sheet_list = dashbords_list.loc[0]['sheet_list']  

    logg(f'-- Экспорт будет из документа {spread_sheet_id} с листа {spread_sheet_list} --', 3)     

    gs_data = spread_sheets.values().get(spreadsheetId=spread_sheet_id, range=spread_sheet_list+'!A:O').execute()
    dd = pd.DataFrame(columns=gs_data['values'][0], data = gs_data['values'][1:])

    cost_price = pd.DataFrame([])  
    cost_price = dd[['Артикул WB', 'Себестоимость', 'Категория', 'Артикул продавца', ]].query('Себестоимость != ""')[1:].reset_index(drop=True)
    cost_price.columns = ['nmid', 'costprice', 'subject', 'supplierarticle']
    cost_price['costprice'] = cost_price['costprice'].str.replace(',', '.').str.replace('₽', '').str.replace(' ', '')
    cost_price['costprice'] = cost_price['costprice'].astype('float')
    cost_price['supplierarticle'] = cost_price['supplierarticle'].str.replace("'", "")
    if len(cost_price) > 0:
        logg(f'В SQL надо загрузить {len(cost_price)} строк из дашбордов GS', 3)
        execute_query(connection, """TRUNCATE TABLE gs_cost_price""", p = False)
        export_to_sql('gs_cost_price', cost_price)
    else:
        logg('!!! Нет данных для загрузки в SQL')
    logg('------  Экспорт данных из GoogleSheets по себестоимости WB завершен ------', 2)    
    return


def elt_gs_costprice_oz():
    """
    Загрузка себестоимости товаров OZ
    Из GS загружаются колонки с A по H, но в итоге остаются только 
    'Ozon Product ID', 'SKU', 'Артикул',  'Себестоимость'
    Проходит переименование колонок в латинские названия 
    Данные эспортируются в SQL, предварительно полностью очистив таблицу    
    """
    logg('------  Начинаю экспорт данных из GoogleSheets по себестоимости OZ ------', 2)   

    dashbords_list = pd.DataFrame([])
    sheet_read = spread_sheets.values().get(spreadsheetId = dash_list_id, range = 'datalens-dashboards!A:D').execute()
    dashbords_list = pd.DataFrame(sheet_read['values'])[1:].reset_index(drop=True)
    dashbords_list.columns = ['dash', 'spread_sheet_url', 'sheet_list', 'type']
    dashbords_list = dashbords_list.query('type == "себестоимость OZ"').reset_index(drop=True)
    dash = dashbords_list.loc[0]['dash']
    spread_sheet_id = dashbords_list.loc[0]['spread_sheet_url'].split('/')[5]
    spread_sheet_list = dashbords_list.loc[0]['sheet_list']  

    logg(f'-- Экспорт будет из документа {spread_sheet_id} с листа {spread_sheet_list} --', 3) 

    gs_data = spread_sheets.values().get(spreadsheetId=spread_sheet_id, range=spread_sheet_list+'!A:H').execute()
    dd = pd.DataFrame(columns=gs_data['values'][0], data = gs_data['values'][1:])

    cost_price = pd.DataFrame([])  
    cost_price = dd[['Ozon Product ID', 'SKU', 'Артикул', 'Себестоимость']].query('Себестоимость != ""')[1:].reset_index(drop=True)
    cost_price.columns = ['ozon_id', 'sku', 'articul', 'costprice']
    cost_price['costprice'] = cost_price['costprice'].str.replace(',', '.').str.replace('₽', '').str.replace(' ', '')
    cost_price['costprice'] = cost_price['costprice'].astype('float')
    cost_price['sku'] = cost_price['sku'].astype('int64')    
    cost_price['ozon_id'] = cost_price['ozon_id'].astype('int64')
    cost_price['articul'] = cost_price['articul'].str.replace("'", "")
    
    if len(cost_price) > 0:
        logg(f'В SQL надо загрузить {len(cost_price)} строк из дашбордов GS', 3)
        execute_query(connection, """TRUNCATE TABLE gs_cost_price_oz""", p = False)
        export_to_sql('gs_cost_price_oz', cost_price)
    else:
        logg('!!! Нет данных для загрузки в SQL')
    logg('------  Экспорт данных из GoogleSheets по себестоимости OZ завершен ------', 2)       

    return

def etl_gs_roadmap():
    """
    Загрузка дорожной карты
    Из GS загружаются только два диапазона ячеек H3:J15 - план и H19:J31 - факт
    Проводится высчисление общей суммы, преобразвание даты
    Вычисляется таблица остатков, как 'факт' - 'план'
    Соединяются справа по месяцам три таблицы 'факт', 'план', 'остатски'
    К полученной общей таблице применяется операция melt - обратная созданию сводной таблицы pivot
    Получается таблица из 5 колонк - 'date', 'month_symbol', 'pf', 'otdel', 'val'
    Проходит переименование колонок и преобазоваание типов для val
    Данные эспортируются в SQL, предварительно полностью очистив таблицу
    """

    logg('------ Начинаю экспорт данных из GoogleSheets дорожной карты ------', 2)


    dashbords_list = pd.DataFrame([])
    sheet_read = spread_sheets.values().get(spreadsheetId = dash_list_id, range = 'datalens-dashboards!A:D').execute()
    dashbords_list = pd.DataFrame(sheet_read['values'])[1:].reset_index(drop=True)
    dashbords_list.columns = ['dash', 'spread_sheet_url', 'sheet_list', 'type']
    dashbords_list = dashbords_list.query('type == "дорожная карта"').reset_index(drop=True)
    dash = dashbords_list.loc[0]['dash']
    spread_sheet_id = dashbords_list.loc[0]['spread_sheet_url'].split('/')[5]
    spread_sheet_list = dashbords_list.loc[0]['sheet_list']  

    logg(f'-- Экспорт будет из документа {spread_sheet_id} с листа {spread_sheet_list} --', 3)


    months = {'Январь' : '01', 
        'Февраль' : '02', 
        'Март' : '03', 
        'Апрель' : '04', 
        'Май' : '05',
        'Июнь' : '06',
        'Июль' : '07',
        'Август' : '08',
        'Сентябрь' : '09',
        'Октябрь' : '10',
        'Ноябрь' : '11',
        'Декабрь' : '12'
        }

    roadmap = pd.DataFrame([]) 
    plan = pd.DataFrame([])
    fact = pd.DataFrame([])
    ostatok = pd.DataFrame([])

    try:
        sheet_read = spread_sheets.values().get(spreadsheetId=spread_sheet_id, range=spread_sheet_list + '!H3:J15').execute()
        plan = pd.DataFrame(sheet_read['values'])[1:].reset_index(drop=True)        
        plan.columns = ['Маркетплейс', 'ОПТ', 'Розница']
        plan = plan.apply(lambda x: x.str.replace(' ', '').str.replace('\xa0', '')).fillna(0).replace('', 0).astype('int')
        plan['Сумма'] = plan['Маркетплейс'] + plan['ОПТ'] + plan['Розница']
        plan['type'] = 'План'
        plan['month'] = plan.index + 1 
        plan['month_symbol'] = plan['month'].astype('str').str.zfill(2) + plan['month'].astype('str').str.zfill(2).replace(dict(zip(months.values(), months.keys())))
        plan['date'] = '2025-' + plan['month'].astype('str').str.zfill(2) + '-01'
        plan.loc[12] = list(plan[['Маркетплейс', 'ОПТ', 'Розница', 'Сумма']].sum()) + ['План', '0', 'Итого', '2025-12-31']          
        plan['date'] = pd.to_datetime(plan['date']).dt.date  
    except Exception as e:
        logg(f'!!! Ошибка при загрузке плановых показателей: {e}')
        plan = pd.DataFrame([])

    try:    
        sheet_read = spread_sheets.values().get(spreadsheetId=spread_sheet_id, range=spread_sheet_list + '!H19:J31').execute()

        fact = pd.DataFrame(sheet_read['values'])[1:].reset_index(drop=True)
        fact.columns = ['Маркетплейс', 'ОПТ', 'Розница']
        fact = fact.apply(lambda x: x.str.replace(' ', '').str.replace('\xa0', '')).fillna(0).replace('', 0).astype('int')
        fact['Сумма'] = fact['Маркетплейс'] + fact['ОПТ'] + fact['Розница']
        fact['type'] = 'Факт'
        fact['month'] = fact.index + 1
        fact['month_symbol'] = fact['month'].astype('str').str.zfill(2) + fact['month'].astype('str').str.zfill(2).replace(dict(zip(months.values(), months.keys())))
        fact['date'] = '2025-' + fact['month'].astype('str').str.zfill(2) + '-01'
        fact.loc[12] = list(fact[['Маркетплейс', 'ОПТ', 'Розница', 'Сумма']].sum()) + ['Факт', '0', 'Итого', '2025-12-31']          
        fact['date'] = pd.to_datetime(fact['date']).dt.date  
    except Exception as e:
        logg(f'!!! Ошибка при загрузке фактических показателей {e}')
        fact = pd.DataFrame([])
        
    if len(plan) > 0 and len(fact) > 0:    
        ostatok['Маркетплейс'] = fact['Маркетплейс'] - plan['Маркетплейс']
        ostatok['ОПТ'] = fact['ОПТ'] - plan['ОПТ']
        ostatok['Розница'] = fact['Розница'] - plan['Розница']
        ostatok['Сумма'] = fact['Сумма'] - plan['Сумма']
        ostatok['type'] = '→Остаток'
        ostatok['month'] = plan['month'] 
        ostatok['month_symbol'] = plan['month_symbol']
        ostatok['date'] = '2025-' + ostatok['month'].astype('str').str.zfill(2) + '-01'
        ostatok.loc[12, 'date'] = '2025-12-31'         
        ostatok['date'] = pd.to_datetime(ostatok['date']).dt.date  
        ostatok = ostatok.dropna(subset=['Сумма'])

        roadmap = pd.concat([plan, fact, ostatok])
        roadmap = roadmap.drop(['month'], axis=1)
        #операция melt - обратная созданию сводной таблицы pivot
        roadmap = roadmap.melt(['date', 'month_symbol', 'type'], value_name='val').reset_index(drop=True)
        roadmap['val'] = roadmap['val'].astype('int64')
        roadmap.columns = ['date', 'month_symbol', 'pf', 'otdel', 'val']
    else:
        roadmap = pd.DataFrame([])
        logg(f'!!! Не могу вычислить отстатки!')
    
    if len(roadmap) > 0:
        logg(f"Надо экспортировать в базу {len(roadmap)} строк", 3)           
        execute_query(connection, """TRUNCATE TABLE gs_roadmap""", p = False)
        export_to_sql('gs_roadmap', roadmap)
    else:
        logg('!!! Нет данных для экспорта в SQL')    
    
    logg('------ Экспорт данных из GoogleSheets дорожной карты завершен ------', 2)
    
    return roadmap



def etl_gs_ext_ads():
    """
    загрузка внешней рекламы
    """
    logg('------ Начинаю экспорт данных из GoogleSheets внешней рекламы ------', 2)

    dashbords_list = pd.DataFrame([])
    sheet_read = spread_sheets.values().get(spreadsheetId = dash_list_id, range = 'datalens-dashboards!A:D').execute()
    dashbords_list = pd.DataFrame(sheet_read['values'])[1:].reset_index(drop=True)
    dashbords_list.columns = ['dash', 'spread_sheet_url', 'sheet_list', 'type']
    dashbords_list = dashbords_list.query('type == "внешняя реклама"').reset_index(drop=True)
    dash = dashbords_list.loc[0]['dash']
    spread_sheet_id = dashbords_list.loc[0]['spread_sheet_url'].split('/')[5]
    #spread_sheet_list = dashbords_list.loc[0]['sheet_list']  

    logg(f'-- Экспорт будет из документа {spread_sheet_id} --', 3)

    spreadsheet = spread_sheets.get(spreadsheetId = spread_sheet_id).execute()
    sheetList = spreadsheet.get('sheets')
    all_sheet_titles = []
    for sheet in sheetList:
        all_sheet_titles.append(sheet['properties']['title'])
    month_sheets_titles = list(filter(re.compile(r"(Январь|Февраль|Март|Апрель|Май|Июнь|Июль|Август|Сентябрь|Октябрь|Ноябрь|Декабрь) (2025|2026|2027)").match, all_sheet_titles))

    logg(f'Всего листов с месячной рекламой {len(month_sheets_titles)}', 3)
    external_ads = pd.DataFrame([])

    for i in range (len(month_sheets_titles)):
        month_sheet = month_sheets_titles[i]
        logg(f'Начинаю обработку листа {month_sheet}', 3)
        try:
            sheet_read = spread_sheets.values().get(spreadsheetId=spread_sheet_id, range=month_sheet + '!A:O').execute()
            reklama = pd.DataFrame(sheet_read['values'])[1:].reset_index(drop=True)
            reklama.columns = ['mp', 'articul', 'nmid_sku', 'shop', 'date', 'maket', 'time',
                               'adv_url', 'adv_contact', 'price', 'period', 'commission', 
                               'coverage', 'repost', 'likes']
            reklama = reklama.dropna(subset=['nmid_sku'])
            reklama = reklama.query('nmid_sku != "" & articul != "" & ~nmid_sku.isna() & ~articul.isna()').reset_index(drop=True)
            reklama['date'] = reklama['date'].replace('', pd.NA)
            reklama['date'] = reklama['date'].fillna(method='ffill')
            reklama['date'] = pd.to_datetime(reklama['date'], format = "%d.%m.%Y").dt.date

            if str(reklama['date'].min()) >= '2025-01-01': 

                reklama['nmid_sku'] = pd.to_numeric(reklama['nmid_sku'], errors='coerce')
                reklama['nmid_sku'] = reklama['nmid_sku'].fillna(0)
                reklama['nmid_sku'] = reklama['nmid_sku'].astype('int64')

                reklama['price'] = reklama['price'].str.replace(',', '.')
                reklama['price'] = pd.to_numeric(reklama['price'], errors='coerce')
                reklama['price'] = reklama['price'].fillna(0)

                reklama['shop'] = reklama['shop'].replace({'Басир' : 'basir', 'Ксения' : 'kseniya', 'Артур' : 'artur'})
                reklama['coverage'] = reklama['coverage'].fillna(method='ffill')
                reklama['coverage'] = pd.to_numeric(reklama['coverage'], errors='coerce')
                reklama['coverage'] = reklama['coverage'].fillna(0)

                reklama['repost'] = reklama['repost'].fillna(method='ffill')
                reklama['repost'] = pd.to_numeric(reklama['repost'], errors='coerce')
                reklama['repost'] = reklama['repost'].fillna(0)

                reklama['likes'] = reklama['likes'].fillna(method='ffill')
                reklama['likes'] = pd.to_numeric(reklama['likes'], errors='coerce')
                reklama['likes'] = reklama['likes'].fillna(0)
                external_ads = pd.concat([external_ads, reklama]) 

                logg(f'Экспортировано {len(reklama)} строк с листа {month_sheet}', 3)
            else:
                logg(f"!!! Пропускаю лист, так как минимальаня дата {str(reklama['date'].min())}", 3)
        except  Exception as e:
            logg(f'!!! Ошибка при обработке листа {month_sheet}: {e}')

    external_ads = external_ads.query('nmid_sku != 0').sort_values(by='date').reset_index(drop=True)

    if len(external_ads) > 0 :
        logg(f'В SQL надо загрузить {len(external_ads)} строк из GS внешней рекламы', 3)

        execute_query(connection, """TRUNCATE TABLE gs_external_ads""", p=False)
        export_to_sql('gs_external_ads', external_ads)
    else:
        logg('Нет данных для экспорта!!!')

    logg('------ Экспорт данных из GoogleSheets внешней рекламы завершен ------', 2)

    return external_ads


def etl_gs_del_product():
    """
    загрузка удаленных товаров
    """
    logg('------ Начинаю экспорт данных из GoogleSheets удаленных товаров ------', 2)

    dashbords_list = pd.DataFrame([])
    sheet_read = spread_sheets.values().get(spreadsheetId = dash_list_id, range = 'datalens-dashboards!A:D').execute()
    dashbords_list = pd.DataFrame(sheet_read['values'])[1:].reset_index(drop=True)
    dashbords_list.columns = ['dash', 'spread_sheet_url', 'sheet_list', 'type']
    dashbords_list = dashbords_list.query('type == "удаленные товары"').reset_index(drop=True)
    dash = dashbords_list.loc[0]['dash']
    spread_sheet_id = dashbords_list.loc[0]['spread_sheet_url'].split('/')[5]
    spread_sheet_list = dashbords_list.loc[0]['sheet_list']  

    logg(f'-- Экспорт будет из документа {spread_sheet_id} с листа {spread_sheet_list} --', 3)


    del_articuls = pd.DataFrame([])    
    try:
        sheet_read = spread_sheets.values().get(spreadsheetId=spread_sheet_id, range=spread_sheet_list + '!A:F').execute()
        del_articuls = pd.DataFrame(sheet_read['values'])[1:]
        del_articuls.columns = ('subject', 'articul', 'nmid', 'url', 'partner2', 'partner')
        del_articuls = del_articuls.dropna(subset=['nmid']).query('nmid != ""').reset_index(drop = True)[['articul', 'nmid']]
        del_articuls['nmid'] = del_articuls['nmid'].astype('int64')
        logg(f'Загружено из GS {len(del_articuls)} строк', 3) 
    
    except  Exception as e:
        logg(f'!!! Ошибка при обработке: {e}')
         
    if len(del_articuls) > 0:
        logg(f'Надо экспортировать в SQL {len(del_articuls)} строк', 3) 

        execute_query(connection, """TRUNCATE TABLE gs_del_articuls""", p=False)
        export_to_sql('gs_del_articuls', del_articuls)
        
    logg('------ Экспорт данных из GoogleSheets удаленных товаров завершен ------', 2)        
    
    return del_articuls


def etl_gs_fenix_product():
    """
    загрузка феникс товаров
    """
    logg('------ Начинаю экспорт данных из GoogleSheets феникс товаров ------', 2)

    dashbords_list = pd.DataFrame([])
    sheet_read = spread_sheets.values().get(spreadsheetId = dash_list_id, range = 'datalens-dashboards!A:D').execute()
    dashbords_list = pd.DataFrame(sheet_read['values'])[1:].reset_index(drop=True)
    dashbords_list.columns = ['dash', 'spread_sheet_url', 'sheet_list', 'type']
    dashbords_list = dashbords_list.query('type == "феникс товары"').reset_index(drop=True)
    dash = dashbords_list.loc[0]['dash']
    spread_sheet_id = dashbords_list.loc[0]['spread_sheet_url'].split('/')[5]
    spread_sheet_list = dashbords_list.loc[0]['sheet_list']  

    logg(f'-- Экспорт будет из документа {spread_sheet_id} с листа {spread_sheet_list} --', 3)


    fenix_articuls = pd.DataFrame([])    
    try:
        sheet_read = spread_sheets.values().get(spreadsheetId=spread_sheet_id, range=spread_sheet_list + '!A:D').execute()
        fenix_articuls = pd.DataFrame(sheet_read['values'])[2:]
        fenix_articuls.columns = ('partner2', 'nmid', 'articul', 'used')
        fenix_articuls = fenix_articuls.dropna(subset=['nmid']).query('nmid != ""').reset_index(drop = True)[['articul', 'nmid']]
        fenix_articuls['nmid'] = fenix_articuls['nmid'].astype('int64')
        logg(f'Загружено из GS {len(fenix_articuls)} строк', 3) 
    
    except  Exception as e:
        logg(f'!!! Ошибка при обработке: {e}')
         
    if len(fenix_articuls) > 0:
        logg(f'Надо экспортировать в SQL {len(fenix_articuls)} строк', 3) 
        #create_sql_table('gs_fenix_articuls', fenix_articuls, 'nmid')
        execute_query(connection, """TRUNCATE TABLE gs_fenix_articuls""", p=False)
        export_to_sql('gs_fenix_articuls', fenix_articuls)
        
    logg('------ Экспорт данных из GoogleSheets феникс товаров завершен ------', 2)        
    
    return fenix_articuls

def etl_gs_otdel_pf():
    """
    План-факт по отделам
    """    
    logg('------ Начинаю экспорт данных из GoogleSheets план-факт по отделам ------', 2)

    dashbords_list = pd.DataFrame([])
    sheet_read = spread_sheets.values().get(spreadsheetId = dash_list_id, range = 'datalens-dashboards!A:D').execute()
    dashbords_list = pd.DataFrame(sheet_read['values'])[1:].reset_index(drop=True)
    dashbords_list.columns = ['dash', 'spread_sheet_url', 'sheet_list', 'type']
    dashbords_list = dashbords_list.query('type == "выручка отделы"').reset_index(drop=True)
    dash = dashbords_list.loc[0]['dash']
    spread_sheet_id = dashbords_list.loc[0]['spread_sheet_url'].split('/')[5]
    spread_sheet_list = dashbords_list.loc[0]['sheet_list']  

    logg(f'-- Экспорт будет из документа {spread_sheet_id} с листа {spread_sheet_list} --', 3)

    otdel_pf_m = pd.DataFrame([])
    try:
        sheet_read = spread_sheets.values().get(spreadsheetId=spread_sheet_id, range=spread_sheet_list+'!B2:K14').execute()
        otdel_pf = pd.DataFrame(sheet_read['values'])[1:].reset_index(drop=True)
        otdel_pf.columns = ['МП_факт', 'МП_план', 'WB_факт', 'WB_план', 'OZ_факт', 'OZ_план', 'ОПТ_факт', 'ОПТ_план', 'Розница_факт', 'Розница_план']
        otdel_pf = otdel_pf.apply(lambda x: x.str.replace(' ', '').str.replace('\xa0', '')).replace('', '0').fillna(0).astype('int64')
        otdel_pf['month'] = otdel_pf.index + 1
        otdel_pf['date'] = otdel_pf['month'].apply(lambda x: '2025-' + str(x).zfill(2) + '-01')
        otdel_pf_m = otdel_pf.melt(['date', 'month'], value_name='val').reset_index(drop=True)
        otdel_pf_m['otdel'] = otdel_pf_m['variable'].apply(lambda x: x.split('_')[0])
        otdel_pf_m['план_факт'] = otdel_pf_m['variable'].apply(lambda x: x.split('_')[1])
    
    except  Exception as e:
        logg(f'!!! Ошибка при обработке: {e}')

    if len(otdel_pf_m) > 0:
        logg(f'Надо экспортировать в SQL {len(otdel_pf_m)} строк', 3) 
        #execute_query(connection, """DROP TABLE gs_otdel_pf""")
        #create_sql_table('gs_otdel_pf', otdel_pf_m, '')
        execute_query(connection, """TRUNCATE TABLE gs_otdel_pf""", p=False)
        export_to_sql('gs_otdel_pf', otdel_pf_m)
    
    logg('------ Экспорт данных из GoogleSheets план-факт по отделам завершен ------', 2)   

    return otdel_pf_m

def forpay_for_gs():
    """
    Импорт Forpay в GS
    """    
    logg('------ Начинаю импорт показателя Forpay в GoogleSheets ------', 2)

    dd = execute_read_query(connection, """
    SELECT
        DATE_TRUNC('day', operation_date::date)::date as date
        , COALESCE(SUM(accruals_for_sale + sale_commission)
                    FILTER (WHERE operation_type_name = 'Доставка покупателю')::NUMERIC(10, 2), 0) as forpay                        
    FROM
        oz_transactions
    WHERE
        operation_date::date BETWEEN NOW()::date - 14 AND NOW()::date - 1
    GROUP BY
        1
    ORDER BY
        1 DESC
    """)
    dd['date'] = dd['date'].astype('str')
    dd['forpay'] = dd['forpay'].astype('float')

    logg(f'-- Построил таблицу forpay за последние дни, начинаю импорт в GS --', 3)

    #Вставляем новую строчку в лист эксельки
    result = spread_sheets.batchUpdate(spreadsheetId = dash_list_id,
    body = {
    'requests': [
        {
            'insertDimension': {
                'range': {
                    'sheetId': 1171988722,
                    'dimension': 'ROWS',
                    'startIndex': 1,
                    'endIndex': 2
                },
                'inheritFromBefore': True
            }
        }
    ]
    }).execute()

    #Вставляем данные за последние 14 дней
    result = spread_sheets.values().batchUpdate(spreadsheetId = dash_list_id, 
        body = {
        "valueInputOption": "USER_ENTERED",
        "data": [
            {"range": "OZ-ForPay!A2:B" + str(len(dd) + 2),
             "majorDimension": "COLUMNS",
             "values":
                [dd['date'].to_list(), 
                dd['forpay'].to_list()]
            }]
    }).execute()

    logg('------ Импорт показателя Forpay в GoogleSheets завершен ------', 2)
    
    return


# ------------------------------------------ 
#              Входная процедура
# ------------------------------------------

def start(event, context):
    """
    Корневая процедура скрипта
    Инициализация подключения к SQL и GS
    запуск по переданному параметру процедур:
        loaddash: elt_gs_load_dash() - загрузка дашбордов сотрудников
        load_stats_dash: elt_gs_load_stats_dash() - загрузка дашбордов статистики
        costprice: elt_gs_costprice() - загрузка себестоимости
        roadmap: etl_gs_roadmap() - загрузка дорожной карты 
        all: все процедуры сразу 
    """
    global log_lev
    log_lev = 0
        
    try:
        command = event['messages'][0]['details']['payload']
    except:
        command = event
    
    print(f'*** Старт скрипта sher_gs_to_sql с параметром {command} в {datetime.now()} ***') 

    gsok = 0
    i = 0
    while i < 5:
        i +=1
        try:
            init_googlesheets()
            i = 10
            gsok = 1
        except:
            print(f'Попытка {i} установить соединение с GoogleSheets провалилась')
            time.sleep(3)

    sqlok = 0
    i = 0
    while i < 5:
        i +=1
        try:
            init_connection()
            base_reset()
            i = 10
            sqlok = 1
        except:
            print(f'Попытка {i} установить соединение с базой SQL провалилась')
            time.sleep(3)

    if gsok == 0 or sqlok == 0: 
        print('Нет соединения с GS или SQL базой!')
    else:
        logg(f'*** Команда для sher_gs_to_sql - [{command}] ***', 1) 
        match command:
            case 'loaddash': elt_gs_load_dash()
            case 'load_stats_dash': elt_gs_load_stats_dash()
            case 'costprice_wb': elt_gs_costprice_wb()
            case 'costprice_oz': elt_gs_costprice_oz()            
            case 'roadmap': etl_gs_roadmap()
            case 'ext_ads': etl_gs_ext_ads() 
            case 'del_prod': etl_gs_del_product()
            case 'fenix_prod': etl_gs_fenix_product()
            case 'otdel_pf': etl_gs_otdel_pf()
            case 'forpay_gs': forpay_for_gs()                  
            case 'all': 
                elt_gs_load_stats_dash()
                elt_gs_load_dash()
                elt_gs_costprice_wb()
                elt_gs_costprice_oz() 
                etl_gs_roadmap()
                etl_gs_ext_ads()
                etl_gs_del_product()
                etl_gs_fenix_product()
                etl_gs_otdel_pf()
            case _: 
                logg('Не задан параметр!') 
        logg(f'*** Выполнена команда - [{command}] ***', 1) 
        connection.close()

    print(f'*** Завершение работы скрипта sher_gs_to_sql в {datetime.now()} ***') 

    return

