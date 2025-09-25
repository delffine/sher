import requests
import pandas as pd
import psycopg2
import numpy as np
from datetime import date, timedelta, datetime
import hashlib
import sys
import time
import re

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
        if log_lev >= 3: print("Connection to PostgreSQL DB successful")
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
    connection.autocommit = True
    cursor = connection.cursor()    
    try:
        cursor.execute(query)
        if log_lev >= 3: 
            if p: logg(f'Query executed successfully {query[0:20]}...')
        result = True    
    except psycopg2.OperationalError as e:
        logg(f"The error occurred: {e}")
        result = False
    return result


# запрос с выводом данных в табличной форме
def execute_read_query(connection, query):
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
    #connection.close()
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
    global head
    global log_lev
    path = ''
    db_set = dict()
    file = open(path + "db_connect.txt")
    for line in file:
        k, v = line.strip().split('=')
        db_set[k] = v
    file.close()
    log_lev = int(db_set['log_level'])    
    connection = create_connection(db_set['dbname'], db_set['uname'], db_set['pass'], db_set['server'], db_set['port'])
    head={}
    head['mir_fandom'] = [db_set['oz_mirfandom_id'], db_set['oz_mirfandom']]
    head['omg'] = [db_set['oz_omg_id'], db_set['oz_omg']]
    head['fandom'] = [db_set['oz_fandom_id'], db_set['oz_fandom']]
    db_set = dict()
    return

def logg(st='', lev=0):  
    st = st.replace("'", "")
    st = st.replace('"', '')    
    if lev <= log_lev: print(f'{st}')
    sql = f"INSERT INTO log (date, script, mess, log_lev) VALUES ('{datetime.now()}', 'oz', '{st}', {lev})"
    execute_query(connection, sql, False)    
    return    

# ------------------------------------------ 
#              ELT скрипты
# ------------------------------------------


def etl_oz_products():
    """
    Данные по товарам из Ozon API предосталяются в виде CSV файла.
    При этом надо сначала сделать запрос на формирование отчета, 
        затем получить ссылку на файл отчета, затем дождаться успешного формирования отчета и скачать его.
    Далее проходит преобработка данных - переименование колонок, 
        привидение типов данных, удаление единичной кавычки.
    Затем по hash (ozon_id sku articul status fbo_available fbs_available price_with_disc) 
        определяются новые записи и строчки, которые надо обновить.
    Наконец, новые данные просто добавляются, обновления обновляются.
    """
    logg(f'---- Начанаю обновление данных по товарам в Ozon ----', 2)
    
    products = pd.DataFrame([])
    url1 = "https://api-seller.ozon.ru/v1/report/products/create"
    data1 = {}

    for h in head:

        headers = {
            "Content-Type": "application/json",
            "Client-Id": head[h][0],
            "Api-Key": head[h][1]
          }
        logg(f'-- Начинаю получение отчета из API для {h} -- ', 2)
        report = pd.DataFrame([])
        response = requests.post(url1, headers=headers, json=data1)  
        if response.status_code == 200:
            rep_code = response.json()['result']['code']
            logg(f'Получен код отчета {rep_code[0:30]}...', 4)
            if rep_code != '':
                i = 0
                while i < 5:
                    logg('Ждем 5 сек...', 3)
                    time.sleep(5)
                    url2 = "https://api-seller.ozon.ru/v1/report/info"
                    data2 = {"code": rep_code}
                    response2 = requests.post(url2, headers=headers, json=data2) 
                    if response2.status_code == 200:
                        if response2.json()['result']['status'] == 'success':
                            report_url = response2.json()['result']['file']
                            logg(f'Получена ссылка на отчет {report_url[8:50]}...', 4)
                            if report_url !='':
                                report = pd.read_csv(report_url, sep=';')
                                logg(f'Получил {len(report)} записей отчета для {h}', 4)
                                break
                            else:
                                logg('!!! Нет ссылки на отчет !!!')
                        else:
                            logg('Отчет еще не готов =(', 3)    
                    else:
                        logg(f'!!! Ошибка получения данных !!!')
                        logg(response2.json())
                        break

                    i +=1
            else:
                logg('!!! Пустой код отчета !!!')
        else:
            logg(f'!!! Ошибка получения кода отчета !!!')
            logg(response.json())
            #break

        if len(report) > 0:
            report['shop'] = h
            products = pd.concat([products, report], ignore_index = True)
        else:
            logg('Нет данных!')
        logg(f'-- Закончил получение отчета для {h}. Всего записей {len(products)} --', 3)

    products = products.rename(columns = {
        'Артикул' : 'articul',
        'Ozon Product ID' : 'ozon_id',
        'SKU' : 'sku',
        'Barcode' : 'barcode',
        'Название товара' : 'name',
        'Контент-рейтинг' : 'content_reiting',
        'Бренд' : 'brand',
        'Статус товара' : 'status',
        'Отзывы' : 'rewiew',
        'Рейтинг' : 'reiting',
        'Видимость на Ozon' : 'visible',
        'Причины скрытия' : 'hide',
        'Дата создания' : 'create_date',
        'Категория комиссии' : 'category',
        'Объем товара, л' : 'volume',
        'Объемный вес, кг' : 'weight',
        'Доступно к продаже по схеме FBO, шт.' : 'fbo_available',
        'Вывезти и нанести КИЗ (кроме Твери), шт' : 'kiz',
        'Зарезервировано, шт' : 'reserved',
        'Доступно к продаже по схеме FBS, шт.' : 'fbs_available',
        'Доступно к продаже по схеме realFBS, шт.' : 'rfbs_available',
        'Зарезервировано на моих складах, шт' : 'my_stock_reserved',
        'Текущая цена с учетом скидки, ₽' : 'price_with_disc',
        'Цена до скидки (перечеркнутая цена), ₽' : 'price',
        'Цена Premium, ₽' : 'price_premium',
        'Размер НДС, %' : 'nds',
        'Количество товара в кванте' : 'count_quant'
    })

    # удаляем одинарную кавычку, чтобы не морочится с экранизацией
    products['articul'] = products['articul'].str.replace("'", "")
    products['name'] = products['name'].str.replace("'", "")
    products['content_reiting'] = products['content_reiting'].str.replace("'", "")
    products['reiting'] = products['reiting'].str.replace("'", "")
    products['volume'] = products['volume'].str.replace("'", "")
    products['weight'] = products['weight'].str.replace("'", "")

    products['hash'] = products.apply(lambda x: 
                    generate_md5_hash(f"{x['ozon_id']}{x['sku']}{x['articul']}\
                    {x['status']}{x['fbo_available']}{x['fbs_available']}{x['price_with_disc']}"), axis=1)   

    #Отбираем конкретные колонки для экспорта в SQL, так как Ozon периодически добавляет новые
    products = products[['articul', 'ozon_id', 'sku', 'barcode', 'name', 'content_reiting',
            'brand', 'status', 'rewiew', 'reiting', 'visible', 'hide',
            'create_date', 'volume', 'weight', 'fbo_available',
            'reserved', 'fbs_available', 'rfbs_available', 'my_stock_reserved',
            'price_with_disc', 'price', 'nds', 'shop', 'hash']]                

    insql = execute_read_query(connection, f"""SELECT ozon_id, sku, hash FROM oz_products""")

    if len(insql) > 0:
        logg('Определяем данные для обновления', 3)
        insql['sku'] = insql['sku'].astype('int64')
        insql['ozon_id'] = insql['ozon_id'].astype('int64') 
        compare = products.merge(insql, 
                   how = 'left', 
                   left_on = ['ozon_id', 'sku'],
                   right_on = ['ozon_id', 'sku'],
                   suffixes = ['_n', '_old']
                  )
        toupdate = compare.query('hash_n != hash_old and ~hash_old.isna()')\
            .drop(['hash_old'], axis=1)\
            .rename(columns = {'hash_n' : 'hash'})
        toinsert = compare.query('hash_old.isna()')\
            .drop(['hash_old'], axis=1)\
            .rename(columns = {'hash_n' : 'hash'})
        
        if len(toinsert) > 0:
            logg(f'--- Добавляем новые данные {len(toinsert)}', 3)
            export_to_sql('oz_products', toinsert)
        else:
            logg('Нет новых строк для добавления!', 3)

        if len(toupdate) > 0:
            logg(f'--- Строк для обновления колонок {len(toupdate)}', 3)
            for i in toupdate.index:
                sql = """UPDATE oz_products SET\n"""
                zap = ''
                for col in toupdate.columns:
                    
                    upd_val = toupdate.loc[i, col]
                    if upd_val == 'nan' or upd_val == 'NaN' or upd_val == 'None':
                        upd_val = 'NULL'
                    else:
                        upd_val = f"'{upd_val}'"
                    sql += f"""{zap}{col} = {upd_val}\n"""
                    zap = ', '
                sql += f"""WHERE ozon_id = '{toupdate.loc[i,'ozon_id']}'
                                 AND sku = '{toupdate.loc[i,'sku']}';\n"""            
                execute_query(connection, sql, p=False)

            logg(f'--- Колонки обновлены ---- ', 3)        
        else:
            logg('Нет строк для обновления данных!', 3)
            
    else:
        logg('--- В базе ничего нет, просто вставляем данные в SQL', 3)
        export_to_sql('oz_products', products)

    logg('------ Обновление товаров OZ завершено ------', 2)
    
    return


def etl_oz_postings(fromdate=''):

    """
    Данные по товарам из Ozon API предосталяются в виде CSV файла.
    При этом надо сначала сделать запрос на формирование отчета, 
        затем получить ссылку на файл отчета, затем дождаться успешного формирования отчета и скачать его.  
    Берем из базы SQL дату самого раннего заказа, который не доставлен и не отменен.
        Запрашиваем по API отчет с этой даты по вчера.
    Далее проходит преобработка данных - переименование колонок, 
        привидение типов данных, удаление единичной кавычки.
    Затем по hash (ozon_id sku articul status fbo_available fbs_available price_with_disc) 
        определяются новые записи и строчки, которые надо обновить.
    Наконец, новые данные просто добавляются, обновления обновляются.
    
    """

    if fromdate == '':
        datefrom = execute_read_query(connection, f"""   
            SELECT  MIN(start_date) FROM oz_postings
                WHERE status != 'Доставлен' AND status != 'Отменён'
            """).loc[0, 'min'][0:10]
        datefrom = datetime.strptime(datefrom, '%Y-%m-%d').date()
        dateto = date.today() - timedelta(days=1)

    else:
        datefrom = datetime.strptime(fromdate, '%Y-%m-%d').date()
        delta_days = 30
        dateto = datefrom + timedelta(days=delta_days)
        if dateto > date.today():
            dateto = date.today() - timedelta(days=1) 

    if (dateto - datefrom).days > 90 : 
        datefrom = dateto - timedelta(days=90) 

    d1 = str(datefrom)    
    d2 = str(dateto)


    logg(f'------ Начинаю обновление отправлений OZ c {d1} по {d2} ------', 2)
    url1 = "https://api-seller.ozon.ru/v1/report/postings/create"
    postings = pd.DataFrame([])
    for fff in ['fbo', 'fbs']:
        data1 = {
            "filter": {
            "processed_at_from": d1 + "T00:00:00.000Z",
            "processed_at_to": d2 + "T23:59:59.999Z",
            "delivery_schema": [fff]
            }
        }

        for h in head:

            headers = {
                "Content-Type": "application/json",
                "Client-Id": head[h][0],
                "Api-Key": head[h][1]
              }
            logg(f'--- Начинаю получение отчета для {h} по {fff} ---', 3)
            report = pd.DataFrame([])
            response = requests.post(url1, headers=headers, json=data1)  
            if response.status_code == 200:
                rep_code = response.json()['result']['code']
                logg(f'Получен код отчета {rep_code[0:30]}...', 3)
                if rep_code != '':
                    i = 0
                    while i < 5:
                        logg('Ждем 5 сек...', 3)
                        time.sleep(5)
                        url2 = "https://api-seller.ozon.ru/v1/report/info"
                        data2 = {"code": rep_code}
                        response2 = requests.post(url2, headers=headers, json=data2) 
                        if response2.status_code == 200:
                            if response2.json()['result']['status'] == 'success':
                                report_url = response2.json()['result']['file']
                                logg(f'Получена ссылка на отчет {report_url[8:50]}...', 3)
                                if report_url !='':
                                    report = pd.read_csv(report_url, sep=';')
                                    logg(f'Получил {len(report)} записей отчета для {h} по {fff}', 3)
                                    break
                                else:
                                    logg(f'!!! Нет ссылки на отчет для {h} по {fff}', 3)
                            else:
                                logg('Отчет еще не готов!', 3)    
                        else:
                            logg(f'!!! Ошибка получения данных для {h} по {fff}')
                            logg(response2.json())
                            break

                        i +=1
                else:
                    logg(f'!!! Пустой код отчета для {h} по {fff}')
            else:
                logg(f'!!! Ошибка получения кода отчета для {h} по {fff}')
                logg(response.json())

            if len(report) > 0:
                report['shop'] = h
                report['fbos'] = fff            
                postings = pd.concat([postings, report], ignore_index = True)
            else:
                logg(f'!!! Нет данных для {h} по {fff}', 3)

    postings = postings.rename(columns = {
        'Номер заказа' : 'order_id',
        'Номер отправления' : 'posting_id',
        'Принят в обработку' : 'start_date',
        'Дата отгрузки' : 'posting_date',
        'Статус' : 'status',
        'Дата доставки' : 'delivery_date',
        'Фактическая дата передачи в доставку' : 'posting_fact_date',
        'Сумма отправления' : 'posting_price',
        'Код валюты отправления' : 'posting_currency',
        'Наименование товара' : 'name',
        'OZON id' : 'sku', #ЭТО SKU !!!!!!!
        'Артикул' : 'articul',
        'Ваша цена' : 'saller_price',
        'Код валюты товара' : 'code_currency',
        'Оплачено покупателем' : 'buyer_price',
        'Код валюты покупателя' : 'buyer_currency',
        'Количество' : 'quanty',
        'Стоимость доставки' : 'delivery_rub',
        'Связанные отправления' : 'chain_posting',
        'Выкуп товара' : 'vykup',
        'Цена товара до скидок' : 'total_price',
        'Скидка %' : 'disconut_pre',
        'Скидка руб' : 'discount_rub',
        'Акции' : 'aktions',
        'Объемный вес товаров, кг' : 'weight',
        'Дата отмены' : 'cancel_date'
    })

    #предобработка перед экспортов в SQL

    
    #удаляем одинарную кавычку, чтобы не морочится с экранизацией
    postings['name'] = postings['name'].str.replace("'", "")
    postings['articul'] = postings['articul'].str.replace("'", "")

    #Кусры валют в 1, как будто везде рубли
    postings['rubcur'] = 1
    
    #Вычсляем HASH для определения уникальности записей
    postings['hash'] = postings.apply(lambda x: 
                    generate_md5_hash(f"{x['posting_id']}{x['order_id']}{x['sku']}\
                    {x['status']}{x['discount_rub']}{x['posting_date']}"), axis=1)   

    #Отбираем конкретные колонки для экспорта в SQL, так как Ozon периодически добавляет новые
    #Сортируем по дате, чтобы записи шли в хронологическом порядке, а не по магазитна
    #postings = postings.sort_values(by = 'start_date').reset_index(drop = True)  
    postings = postings[[
        'order_id', 'posting_id', 'start_date', 'posting_date', 'status',
        'delivery_date', 'posting_fact_date', 'posting_price',
        'posting_currency', 'name', 'sku', 'articul', 'saller_price',
        'code_currency', 'buyer_price', 'buyer_currency', 'quanty',
        'total_price', 'disconut_pre', 'discount_rub', 'aktions',
         'shop', 'fbos', 'cancel_date', 'hash', 'rubcur'
                       ]].sort_values(by = 'start_date').reset_index(drop = True)  

    logg(f'--- Закончил получение отчета. Всего записей {len(postings)} ---', 3)       

    logg('--- Определяем данные, которые уже есть в SQL ---', 3)

    date_from = postings['start_date'].min()[0:10]
    date_to = postings['start_date'].max()[0:10]

    insql = execute_read_query(connection, f"""
            SELECT posting_id, order_id, sku, hash FROM oz_postings 
                WHERE start_date::date BETWEEN '{date_from}'::date AND '{date_to}'::date
        """)

    if len(insql) > 0:
        logg('Определяем данные для обновления', 3)
        insql['sku'] = insql['sku'].astype('int64') 
        compare = postings.merge(insql, 
                   how = 'left', 
                   left_on = ['posting_id', 'order_id', 'sku'],
                   right_on = ['posting_id', 'order_id', 'sku'],
                   suffixes = ['_n', '_old']
                  )
        toupdate = compare.query('hash_n != hash_old and ~hash_old.isna()')\
            .drop(['hash_old'], axis=1)\
            .rename(columns = {'hash_n' : 'hash'})
        toinsert = compare.query('hash_old.isna()')\
            .drop(['hash_old'], axis=1)\
            .rename(columns = {'hash_n' : 'hash'})
        
        if len(toinsert) > 0:
            logg(f'--- Добавляем новые данные {len(toinsert)}', 3)
            export_to_sql('oz_postings', toinsert)
        else:
            logg('Нет новых данных для добавления!', 3)

        if len(toupdate) > 0:
            logg(f'--- Строк для обновления колонок {len(toupdate)}', 3)

            for i in toupdate.index:
                sql = """UPDATE oz_postings SET\n"""
                zap = ''
                for col in toupdate.columns:
                    
                    upd_val = toupdate.loc[i, col]
                    if upd_val == 'nan' or upd_val == 'NaN' or upd_val == 'None':
                        upd_val = 'NULL'
                    else:
                        upd_val = f"'{upd_val}'"
                    sql += f"""{zap}{col} = {upd_val}\n"""
                    zap = ', '
                sql += f"""WHERE posting_id = '{toupdate.loc[i,'posting_id']}'
                                 AND order_id = '{toupdate.loc[i,'order_id']}'
                                 AND sku = '{toupdate.loc[i,'sku']}';\n"""            
                execute_query(connection, sql, p=False)
            logg(f'--- Колонки в строках обновлены ---- ', 3)        
        else:
            logg('Нет строк для обновления данных!', 3)


    else:
        logg('--- Все полученные данные новые. Просто вставляем в SQL', 3)
        export_to_sql('oz_postings', postings)

    logg('------ Обновление отправлений OZ завершено ------', 2)
    
    return


def etl_oz_returns(fromdate=''):
    """
    Обновление возвратов на OZ
    """

    if fromdate == '':
        datefrom = execute_read_query(connection, f"""SELECT MAX(return_date) FROM oz_returns""").loc[0, 'max'][0:10]
        datefrom = datetime.strptime(datefrom, '%Y-%m-%d').date()
        dateto = date.today() - timedelta(days=1)

    else:
        datefrom = datetime.strptime(fromdate, '%Y-%m-%d').date()
        delta_days = 30
        dateto = datefrom + timedelta(days=delta_days)
        if dateto > date.today():
            dateto = date.today() - timedelta(days=1) 

    d1 = str(datefrom)    
    d2 = str(dateto)

    returns = pd.DataFrame([])

    logg(f'------ Начинаю обновление возвратов OZ c {d1} по {d2} ------', 2)
    for h in head:

        headers = {
            "Content-Type": "application/json",
            "Client-Id": head[h][0],
            "Api-Key": head[h][1]
          }

        last_id = 0
        logg(f'Получаю возвраты для {h} с {d1} по {d2}', 3)        
        while True:
            url = "https://api-seller.ozon.ru/v1/returns/list"
            data = {"limit": 500,
                    "last_id" : str(last_id),
                    "filter" :
                       {"logistic_return_date" : 
                           {
                            "time_from" :  d1 + "T00:00:00.000Z",
                            "time_to" : d2 + "T23:59:59.999Z"
                           }
                       }
                   }
            response = requests.post(url, headers=headers, json=data)  
            if response.status_code == 200:
                dd = pd.DataFrame(response.json()['returns'])
                dd['shop'] = h
                returns = pd.concat([returns, dd], ignore_index = True)
                logg(f'Всего получено возвратов {len(returns)}', 3)
                if len(dd) < 500: break
                last_id = dd.iloc[-1]['id']
            else:
                logg(f'Ошибка получения данных')
                logg(response.json())
                break
    
    # "Раскрываем" колонки, в которых json в json. 
    # Добавляем префикс, чтобы новые колонки не дублировались
    # При этом исходная колонка удаляется.
    # Берем НЕ все колонки, так как много лишних данных 
    logg(f'--- Начинаю предобработку данных перед экспортом в SQL ---', 3)
    returns = pd.concat([returns.drop(['product', 'exemplars', 'place', 'target_place', 'visual', 'storage', 'logistic', 'additional_info'], axis=1), 
                    pd.json_normalize(returns['product']).add_prefix('pr_'),
                    pd.json_normalize(returns['logistic']).add_prefix('lg_'),
                        ], axis=1)

    # Преобразование типов для корректного экспорта в SQL 
    returns['id'] = returns['id'].astype('int64')
    returns['pr_sku'] = returns['pr_sku'].astype('int64')
    returns['company_id'] = returns['company_id'].astype('int64')
    returns['schema'] = returns['schema'].str.lower()

    # Берем не все колонки из ответа OZ API, а только нужные!
    returns = returns[['id', 'lg_return_date', 'return_reason_name', 'type', 'schema', 'order_number',  
            'pr_sku', 'pr_offer_id', 'pr_name', 'pr_commission_percent', 'pr_quantity', 'pr_price.currency_code', 
            'pr_price.price', 'pr_price_without_commission.currency_code', 'pr_price_without_commission.price', 
            'pr_commission.currency_code', 'pr_commission.price', 'company_id', 'shop']]
    
    # Переименование колонок для удаления префикса и соответствия с другими таблицами
    returns = returns.rename(columns = {'id' : 'return_id', 'lg_return_date' : 'return_date', 
            'order_number' : 'order_id', 'pr_sku' : 'sku', 'pr_offer_id' : 'articul', 'pr_name' : 'name', 'schema' : 'fbos', 
            'pr_quantity' : 'quantity', 'pr_commission_percent' : 'commission_percent',
            'pr_price.currency_code' : 'price_cur', 'pr_price.price' : 'price',
            'pr_price_without_commission.currency_code' : 'price_without_com_cur', 'pr_price_without_commission.price' : 'price_without_com',
            'pr_commission.currency_code' : 'com_cur', 'pr_commission.price' : 'com_price'
                             })

    #сортируем по дате, чтобы записи шли в хронологическом порядке, а не по магазинам
    returns = returns.sort_values(by = 'return_date').reset_index(drop = True)     


    logg(f'--- Начинаю экспорт данных в SQL ---', 3)
    
    # Так как обновление возвратов идет за последние 14 дней, 
    # НЕ делаем очищение таблицы, чтобы остались более ранние записи
    # execute_query(connection, """TRUNCATE TABLE oz_returns""")
    export_to_sql('oz_returns', returns)

    logg(f'------ Обновление возвратов OZ завершено ------', 2)
    return


def etl_oz_stocks():
    """
    Обновление остатков на OZ
    """
    logg('------ Начинаю обновление остатков FBO на OZ ------', 2)

    url = "https://api-seller.ozon.ru/v1/analytics/stocks"
    fbo_stocks = pd.DataFrame([])
    
    #Определяют sku, которые были в продажах за все время (??? может поставить ограничение - за посление 3-6 месяцев?)
    prod = execute_read_query(connection, f"""SELECT DISTINCT sku, shop FROM oz_postings WHERE fbos = 'fbo'""")  
    
    for h in head:
        headers = {
            "Content-Type": "application/json",
            "Client-Id": head[h][0],
            "Api-Key": head[h][1]
          }
        
        skus = prod.query('shop == @h')['sku'].to_list()
        logg(f'-- Запрашиваю остатки для {len(skus)} товаров по магазину {h} --', 3)
        i = 0
        
        #Запрос остатков возможен только по 100 sku, 
        #поэтому делаем циклический опрос
        while i < len(skus):
            sss = skus[i: i + 100]
            i += 100

            data = {"skus": sss}
            response = requests.post(url, headers=headers, json=data)  
            if response.status_code == 200:
                dd = pd.DataFrame(response.json()['items'])
                dd['shop'] = h
                fbo_stocks = pd.concat([fbo_stocks, dd], ignore_index = True)        
                logg(f'Получил {len(dd)} строк по магазину {h}', 3)
            else:
                logg(f'!!! Ошибка получения данных')
                logg(response.json())

    logg(f'Всего получено строк {len(fbo_stocks)}', 3)

    fbo_stocks = fbo_stocks.rename(columns = {'offer_id' : 'articul'})

    fbo_stocks['articul'] = fbo_stocks['articul'].str.replace("'", "")
    fbo_stocks['name'] = fbo_stocks['name'].str.replace("'", "")
    
    
    #Замена значений причин возвратов по словарю
    status_dic = {
        'UNSPECIFIED' : 'Значение не определено',
        'TURNOVER_GRADE_NONE' : 'Нет статуса ликвидности',
        'DEFICIT' : 'Дефицитный. Остатков товара хватит до 28 дней',
        'POPULAR' : 'Очень популярный. Остатков товара хватит на 28–56 дней',
        'ACTUAL' : 'Популярный. Остатков товара хватит на 56–120 дней',
        'SURPLUS' : 'Избыточный. Товар продаётся медленно, остатков хватит более чем на 120 дней',
        'NO_SALES' : 'Без продаж. У товара нет продаж последние 28 дней',
        'WAS_NO_SALES' : 'Был без продаж. У товара не было продаж и остатков последние 28 дней',
        'RESTRICTED_NO_SALES' : 'Без продаж, ограничен. У товара не было продаж более 120 дней. Такой товар нельзя добавить в поставку',
        'COLLECTING_DATA' : 'Сбор данных. Для расчёта ликвидности нового товара собираем данные в течение 60 дней после поставки',
        'WAITING_FOR_SUPPLY' : 'Ожидаем поставки. На складе нет остатков, доступных к продаже. Сделайте поставку для начала сбора данных',
        'WAS_DEFICIT' : 'Был дефицитным. Товар был дефицитным последние 56 дней. Сейчас у него нет остатков',
        'WAS_POPULAR' : 'Был очень популярным. Товар был очень популярным последние 56 дней. Сейчас у него нет остатков',
        'WAS_ACTUAL' : 'Был популярным. Товар был популярным последние 56 дней. Сейчас у него нет остатков',
        'WAS_SURPLUS' : 'Был избыточным. Товар был избыточным последние 56 дней. Сейчас у него нет остатков'
    }

    fbo_stocks['turnover_grade_rus'] = fbo_stocks['turnover_grade'].replace(status_dic)
    fbo_stocks['turnover_grade_cluster_rus'] = fbo_stocks['turnover_grade_cluster'].replace(status_dic)

    #Отбираем конкретные колонки для экспорта в SQL, так как Ozon периодически добавляет новые
    fbo_stocks = fbo_stocks[['sku', 'name', 'articul', 'warehouse_id', 'warehouse_name',
       'cluster_id', 'cluster_name', 'item_tags', 'ads', 'days_without_sales',
       'turnover_grade', 'idc', 'available_stock_count', 'valid_stock_count',
       'waiting_docs_stock_count', 'expiring_stock_count',
       'transit_defect_stock_count', 'stock_defect_stock_count',
       'excess_stock_count', 'other_stock_count', 'requested_stock_count',
       'transit_stock_count', 'return_from_customer_stock_count',
       'return_to_seller_stock_count', 'idc_cluster', 'ads_cluster',
       'turnover_grade_cluster', 'days_without_sales_cluster', 'shop',
       'turnover_grade_rus', 'turnover_grade_cluster_rus']]
       

    #Полностью очищаем таблицу SQL и перезаписываем значения
    if len(fbo_stocks) > 0:
        logg(f'-- Надо перезаписать {len(fbo_stocks)} строк в SQL --', 3)        
        execute_query(connection, """TRUNCATE TABLE oz_fbo_stocks""")
        export_to_sql('oz_fbo_stocks', fbo_stocks)
    else:
        logg(f'-- Нет данных для записи в SQL --', 3)

    logg('------ Обновление остатков FBO на OZ завершено ------', 2)
    
    return

def etl_oz_prod_actions():
    """
    Сначала запрашивается список действующих акций
    Затем для каждого магазина, для каждого номера акции по 100 строк идет запрос товаров
        получается три цикла в цикле
    К полученной таблице по одному магазину и одной акции данным прибавляются строчки с 
        номером акции, названием акции, датой старта, датой завершения
    Все эти данные собираются в одну итоговую таблицу
    Затем данные просто перезаписываются в SQL 
    """       
    logg('------ Начинаю обновление товаров в акция OZ ------', 2)

    url = "https://api-seller.ozon.ru/v1/actions/products"
    prod_actions = pd.DataFrame([])
    last_len = 0
    for h in head:
        headers = {
            "Content-Type": "application/json",
            "Client-Id": head[h][0],
            "Api-Key": head[h][1]
          }
        #запрашиваем действующие на сейчас акции
        response = requests.get('https://api-seller.ozon.ru/v1/actions', headers=headers) 
        actions = pd.DataFrame(response.json()['result'])

        #запрашиваем участвующие в акции товары
        for a in range(len(actions)):
            last_id = ''
            i = 0
            while i < 100:
                data = {
                    "action_id": int(actions.iloc[a]['id']),
                    "limit": 100,
                    "last_id": last_id
                    }

                response = requests.post(url, headers=headers, json=data)  
                if response.status_code == 200:
                    result = pd.DataFrame(response.json()['result']['products'])

                    if len(result) > 0:    
                        result['action_id'] = actions.iloc[a]['id']
                        result['action_name'] = actions.iloc[a]['title']
                        result['action_start'] = actions.iloc[a]['date_start']
                        result['action_end'] = actions.iloc[a]['date_end']                
                        result['shop'] = h
                        prod_actions = pd.concat([prod_actions, result], ignore_index=True)

                else:
                    logg(f'Ошибка получения данных')
                    logg(response.json())
                    break

                if len(result) < 100: break
                last_id = response.json()['result']['last_id']
                i +=1

            logg(f"Полученно {len(prod_actions) - last_len} товаров по акции <{actions.iloc[a]['title']}> ({actions.iloc[a]['id']}) для магазина {h}", 3)
            last_len = len(prod_actions)
            # ------------ конец while
            
        # ------------ конец for action
    
    # ------------ конец for head
    
    logg(f'Всего получено записей {len(prod_actions)}', 3)

    prod_actions = prod_actions.rename(columns = {'id' : 'ozon_id'})

    #Отбираем конкретные колонки для экспорта в SQL, так как Ozon периодически добавляет новые
    prod_actions = prod_actions[['ozon_id', 'price', 'action_price', 'max_action_price', 'add_mode',
            'stock', 'min_stock', 'alert_max_action_price_failed',
            'alert_max_action_price', 'action_id', 'action_name', 'action_start',
            'action_end', 'shop']]


    #Экспортируем данные в SQL
    if len(prod_actions) > 0:
        logg(f'-- Надо перезаписать {len(prod_actions)} строк в SQL --', 3)        
        
        #данные по ценам и способу включения постоянно меняются, поэтому чистим таблицу 
        execute_query(connection, """TRUNCATE TABLE oz_prod_actions""")
         
        export_to_sql('oz_prod_actions', prod_actions)
    else:
        logg(f'-- Нет данных для записи в SQL --', 3)
                  
    logg('------ Обновление товаров в акция OZ завершено ------', 2)
    return

def elt_oz_discounts():
    """
    Обновление заявок на скидки
    Запрос идет в цикле по 50 строк в обратном порядке от "вчера"
    до максимальной даты, которая есть в SQL
    Далее проходит преобработка данных - переименование колонок, 
        привидение типов данных, удаление единичной кавычки.
    Затем по hash (discount_id, sku, status, approved_price, moderated_at) 
        определяются новые записи и строчки, которые надо обновить.
    Наконец, новые данные просто добавляются, обновления обновляются.
    """    
    logg('------ Начинаю обновление запросов на скидки OZ ------', 2)
    discounts = pd.DataFrame([])
    last_len = 0
    for h in head:
        if h == 'mir_fandom': continue
        headers = {
            "Content-Type": "application/json",
            "Client-Id": head[h][0],
            "Api-Key": head[h][1]
          }
        maxdate = execute_read_query(connection, f"""SELECT MAX(created_at) 
                    FROM oz_discounts WHERE shop = '{h}'""").loc[0]['max'][0:10]
        #maxdate = '2025-01-01'
        logg(f'Запрашиваю запросы на скидки по магазину {h} до даты {maxdate}', 3)

        i = 1
        while i < 100:
            url = "https://api-seller.ozon.ru/v1/actions/discounts-task/list"
            data = {
                "status": "UNKNOWN",
                "page": i,
                "limit": 50
            }
            response = requests.post(url, headers=headers, json=data)  
            if response.status_code == 200:
                dd = pd.DataFrame(response.json()['result'])
                dd['shop'] = h
                discounts = pd.concat([discounts, dd], ignore_index=True)    
            else:
                logg(f'Ошибка получения данных')
                logg(response.json())
                break

            if i % 20 == 0: 
                logg(f'Получил записей {i * 50}', 3)

            if dd['created_at'].min()[0:10] < maxdate:
                logg(f"Достигнута максимальная в SQL дата: {dd['created_at'].max()[0:10]}", 3)
                i = 1000

            i +=1
        logg(f'Получил {len(discounts) - last_len} запросов на скидку для магазина {h}', 3)
        last_len = len(discounts)

    #Предобработка данных

    discounts = discounts.rename(columns = {'offer_id' : 'articul', 'id' : 'discount_id'})  
    discounts['articul'] = discounts['articul'].str.replace("'", "")
    discounts['approved_price_fee_percent'] = pd.to_numeric(discounts['approved_price_fee_percent'], 
                                        errors='coerce').fillna(0).astype('int64')

    discounts['hash'] = discounts.apply(lambda x: 
                    generate_md5_hash(f"{x['discount_id']}{x['sku']}{x['status']}\
                    {x['approved_price']}{x['moderated_at']}"), axis=1)    

    #Отбираем конкретные колонки для экспорта в SQL, так как Ozon периодически добавляет новые
    #Сортируем по дате, чтобы записи шли в хронологическом порядке, а не по магазитна
    discounts = discounts[[
                'discount_id', 'created_at', 'end_at', 'edited_till', 'status',
                'customer_name', 'sku', 'user_comment', 'seller_comment',
                'requested_price', 'approved_price', 'original_price', 'discount',
                'discount_percent', 'base_price', 'min_auto_price', 'prev_task_id',
                'is_damaged', 'moderated_at', 'approved_discount',
                'approved_discount_percent', 'is_purchased', 'is_auto_moderated',
                'articul', 'email', 'first_name', 'last_name', 'patronymic',
                'approved_quantity_min', 'approved_quantity_max',
                'requested_quantity_min', 'requested_quantity_max',
                'requested_price_with_fee', 'approved_price_with_fee',
                'approved_price_fee_percent', 'shop', 'hash'
       ]].sort_values(by='created_at').reset_index(drop=True)

            
    insql = execute_read_query(connection, f"""SELECT discount_id, sku, hash FROM oz_discounts""")

    if len(insql) > 0:
        logg('Определяем данные для обновления', 3)
        insql['sku'] = insql['sku'].astype('int64')
        insql['discount_id'] = insql['discount_id'].astype('int64') 
        compare = discounts.merge(insql, 
                   how = 'left', 
                   left_on = ['discount_id', 'sku'],
                   right_on = ['discount_id', 'sku'],
                   suffixes = ['_n', '_old']
                  )
        toupdate = compare.query('hash_n != hash_old and ~hash_old.isna()')\
            .drop(['hash_old'], axis=1)\
            .rename(columns = {'hash_n' : 'hash'})
        toinsert = compare.query('hash_old.isna()')\
            .drop(['hash_old'], axis=1)\
            .rename(columns = {'hash_n' : 'hash'})

        if len(toinsert) > 0:
            logg(f'--- Добавляем новые данные {len(toinsert)}', 3)
            export_to_sql('oz_discounts', toinsert)
        else:
            logg('Нет новых строк для добавления!', 3)

        if len(toupdate) > 0:
            logg(f'--- Строк для обновления колонок {len(toupdate)}', 3)
            for i in toupdate.index:
                sql = """UPDATE oz_discounts SET\n"""
                zap = ''
                for col in toupdate.columns:
                    upd_val = toupdate.loc[i, col]
                    if upd_val == 'nan' or upd_val == 'NaN' or upd_val == 'None':
                        upd_val = 'NULL'
                    else:
                        upd_val = f"'{upd_val}'"
                    sql += f"""{zap}{col} = {upd_val}\n"""
                    zap = ', '
                sql += f"""WHERE discount_id = '{toupdate.loc[i,'discount_id']}'
                                 AND sku = '{toupdate.loc[i,'sku']}';\n"""            
                execute_query(connection, sql, p=False)
            logg(f'\n--- Колонки обновлены ---- ', 3)        
        else:
            logg('Нет строк для обновления данных!', 3)

    else:
        logg('--- В базе ничего нет, просто вставляем данные в SQL', 3)
        export_to_sql('oz_discounts', discounts)

    logg('------ Обновление запросов на скидки OZ завершено ------', 2)

    return

def elt_oz_transaction(fromdate=''):
    """
    Список транзакций получается с последенй даты до вчера
    По магазинам в цикле по 1000 записей.
    Затем происходит не стандартное раскрытире json в колонках, а другой подход.
    Из колонки posting, преобразованной в строку, через регулярное выражение 
    извлекается номер отправления, он же номер заказа order_id
    Аналогично, через регулярное выражение из колонки items извлекаются sku товаров, 
    которые были в этом отправлении.
    Аналогично, через регулярное выражение из колонки service извлекаются стоимость 
    price услуг, которая начислена на это отправление.
    Вычисляется количество товаров в отправлении.
    В последствии делением количества товаров на стомость услуг или стоимость отправления 
    будет определяться стоисмть в расчете на товар. Это надо для отправлений, в которых
    более одного товара. В отправлениях где один товар просто все поделиться на 1.
    Далее json колонки преобразуется в строчки, удаляюся кавычки.
    Наконец, "обогащеные" данные импортируются в SQL
    """    
    logg('------ Начинаю обновление транзакций OZ ------', 2)
    
    url = "https://api-seller.ozon.ru/v3/finance/transaction/list"
    transactions = pd.DataFrame([])
    for h in head:
        headers = {
            "Content-Type": "application/json",
            "Client-Id": head[h][0],
            "Api-Key": head[h][1]
          }
        
        if fromdate == '':
            datefrom = execute_read_query(connection, f"""SELECT  MAX(operation_date) 
                        FROM oz_transactions WHERE shop = '{h}'""")['max'][0][0:10]
            datefrom = datetime.strptime(datefrom, '%Y-%m-%d').date()
            dateto = date.today() - timedelta(days=1)
            if (dateto - datefrom).days > 28:
                delta_days = 28
                dateto = datefrom + timedelta(days=delta_days)
        else:
            datefrom = datetime.strptime(fromdate, '%Y-%m-%d').date()
            dateto = date.today() - timedelta(days=1)
            if (dateto - datefrom).days > 28:
                delta_days = 28
                dateto = datefrom + timedelta(days=delta_days)    
        d1 = str(datefrom)    
        d2 = str(dateto)


        logg(f'Запрашиваю отчет по {h} c {d1} по {d2}', 3)
        i = 1
        result = pd.DataFrame([])
        while True:
            data = {
                "filter": {
                    "date": {
                        "from": str(d1) + "T00:00:00.000Z",
                        "to": str(d2) + "T23:59:59.999Z"
                    },
                },
                "page": i,
                "page_size": 1000
                }
            logg(f'Запрашиваю {i} страницу отчета для {h}', 3)
            response = requests.post(url, headers=headers, json=data)  
            if response.status_code == 200:

                dd = pd.DataFrame(response.json()['result']['operations'])
                result = pd.concat([result, dd], ignore_index=True)

            else:
                logg(f'Ошибка получения данных')
                logg(response.json())


            if len(dd) < 1000: 
                break
            else: 
                i +=1
        result['shop'] = h
        # ---- конец while перебора страниц
        
        transactions = pd.concat([transactions, result], ignore_index=True)
        logg(f'Всего получил {len(result)} строк отчета для {h}', 3)
    # ----- конец for перебора магазинов

    logg(f'Всего получено записей {len(transactions)}', 3)   

    transactions['order_id'] = transactions['posting'].apply(lambda x: ''.join(re.findall(r"'posting_number': '(\d+-\d+)", str(x))))
    transactions['skus'] = transactions['items'].apply(lambda x: ', '.join(set(re.findall(r"'sku': (\d+)", str(x)))))
    transactions['quantity'] = transactions['items'].apply(lambda x: len(re.findall(r"'sku': (\d+)", str(x))))
    transactions['service_amount'] = transactions['services'].apply(lambda x: sum(list(map(float, re.findall(r"'price': ([-0-9.]+)", str(x))))))

    transactions['posting'] = transactions['posting'].astype('str').str.replace("'", "")
    transactions['items'] = transactions['items'].astype('str').str.replace("'", "")
    transactions['services'] = transactions['services'].astype('str').str.replace("'", "")

    #Отбираем конкретные колонки для экспорта в SQL, так как Ozon периодически добавляет новые
    #Сортируем по дате, чтобы записи шли в хронологическом порядке, а не по магазитна
    transactions = transactions[[
                'operation_id', 'operation_type', 'operation_date',
                'operation_type_name', 'delivery_charge', 'return_delivery_charge',
                'accruals_for_sale', 'sale_commission', 'amount', 'type', 'posting',
                'items', 'services', 'order_id', 'skus', 'quantity', 'service_amount',
                'shop'
                ]].sort_values(by='operation_date').reset_index(drop=True)
    
    if len(transactions) > 0:
        logg(f'--- Добавляем в базу новые трназакций {len(transactions)}', 3)
        export_to_sql('oz_transactions', transactions)
    else:
        logg('!!! Нет записей для добавления!', 3)

    logg('------ Обновление транзакций OZ завершено ------', 2)
    
    return

def etl_currency():
    """
    Обновление курсов валют с сайта центробанка
    Курсы валют нунжы для выкупов, которые сделаны не за рубли
    Пока это Беларусь и Казахстан, но берутся также Армения, Узбекистан и Кыркызстан
    """
    logg(f'------ Начинаю обновление курсов валют ------ ', 2)  
    currency = pd.DataFrame([])
    miss_date = execute_read_query(connection, f"""
        WITH date_range AS (
            SELECT 
                generate_series(
                    '2025-01-01'::date,  -- Начальная дата диапазона
                    NOW()::date,  -- Конечная дата диапазона
                    '1 day'::interval
                )::date AS missing_date
        ),
        existing_dates AS (
            SELECT DISTINCT date  
            FROM currency
        )
        SELECT d.missing_date
        FROM date_range d
        LEFT JOIN existing_dates e ON d.missing_date = e.date::date
        WHERE e.date IS NULL
        ORDER BY d.missing_date
        """)
    
    if len(miss_date) > 0:   
        for d in miss_date['missing_date']:
            cur_on_date = d.strftime('%d/%m/%Y')
            logg(f'Запрашиваю курсы валют на {cur_on_date}', 3)    
            #try:
            dd = pd.read_xml(f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={cur_on_date}', encoding='windows-1251')
            ccc = ('BYN', 'KZT', 'AMD', 'KGS', 'UZS')
            cur = dd.query('CharCode.isin(@ccc)')[['CharCode', 'Nominal', 'Value']]
            cur['Value'] = cur['Value'].str.replace(',', '.').astype('float')
            cur['rubcur'] = round(cur['Value'] / cur['Nominal'], 4)
            cur['date'] = d.strftime('%Y-%m-%d')
            currency = pd.concat([currency, cur], ignore_index=True)
            time.sleep(2)
            #except:
            #    print('Ошибка при получении курсов валют!')
    else:
        logg('Курсы валют есть на все даты. Запрашивать нечего!', 3)

    if len(currency) > 0:
        currency.columns = currency.columns.str.lower()
        export_to_sql('currency', currency)
        logg('Обновляю курсы в таблице отправлений', 3)
        execute_query(connection, f"""
            UPDATE
                oz_postings
            SET
                rubcur = COALESCE((
                    SELECT 
                        rubcur 
                    FROM 
                        currency
                    WHERE 
                        currency.date::date = oz_postings.start_date::date
                        AND currency.charcode = oz_postings.buyer_currency
                        ), 1)::NUMERIc(10, 2)
            WHERE 
                buyer_currency != 'RUB'
                AND start_date::date >= NOW() - INTERVAl '10 days'
            """)
    else:
        logg('Нет данных для обновления SQL', 3)

    logg(f'------ Обновление курсов валют завершено ------ ', 2)  
    return



def etl_oz_category():
    """
    Категории товаров, которые надо доставать через три запроса
    """
    logg(f'------ Начинаю обновление категорий товаров ------ ', 2)  

    url1 = "https://api-seller.ozon.ru/v1/description-category/tree"
    headers = {
        "Content-Type": "application/json",
        "Client-Id": head['omg'][0],
        "Api-Key": head['omg'][1]
      }
    data = {}
    report = pd.DataFrame([])
    response = requests.post(url1, headers=headers, json=data) 
    cat_tree_oz = response.json()

    url1 = "https://api-seller.ozon.ru/v3/product/info/list"
    prod = pd.DataFrame([])
    for h in head:
            logg(f"Отбираю sku магазина {h}", 3)    
            pp = execute_read_query(connection, f"""
                SELECT
                    DISTINCT sku
                FROM
                    oz_products as p
                WHERE
                    shop = '{h}'
                    AND sku > 0
                """)
            sku_list = pp['sku'].astype('str').to_list()
            l = len(sku_list)

            logg(f"Всего надо проверить {l} sku от магазина {h}", 3)
            headers = {
                "Content-Type": "application/json",
                "Client-Id": head[h][0],
                "Api-Key": head[h][1]
              }
            
            di = 100 
            for i in range(l//di + 1):
                data = {
                    'sku': sku_list[i * di: i * di + di]
                }
                #logg(f"Запрашиваю SKU с {i * di} по {i * di + di}", 3)
                report = pd.DataFrame([])
                try:
                    response = requests.post(url1, headers=headers, json=data)  
                    if response.status_code == 200:
                        pp = pd.DataFrame(response.json()['items'])
                        prod = pd.concat([prod, pp])
                    else:
                        logg(f'Ошибка получения данных')
                        #break
                except Exception as e:
                    logg(f'!!! Ошибка при запросе категорий товаров: {e}')     
                time.sleep(2)                    

    cb = prod.groupby(['description_category_id', 'type_id'], as_index=False)['sku'].count().sort_values(by=['description_category_id', 'type_id'])

    logg(f"Всего категорий {len(cb)}. Ищу названия", 3) 

    cat_dic = {}
    for i in range(len(cb)):
        cat_name = re.findall("{'type_name': '([А-Яа-я ,]+)', 'type_id': " + str(cb.iloc[i]['type_id']), str(cat_tree_oz))   
        cat_dic[cb.iloc[i]['type_id']]  = cat_name[0]
    
    """
    cat_dic = {
        92697: 'Кондиционер для стирки белья',
        970801117: 'Парфюмерный спрей для дома',
        93903: 'Соль для ванны',
        970955230: 'Подарочный набор',
        970671314: 'Коробка, туба подарочная',
        92715: 'Ароматизатор автомобильный',
        92718: 'Ароматический диффузор',
        971053339: 'Блоттер',
        970681400: 'Наливная парфюмерия',
        93244: 'Футболка',
        568641099: 'Наполнитель для ароматического диффузора',
        93904: 'Спрей для ухода за кожей',
        93404: 'Набор парфюмерный',
        92721: 'Саше',
        93885: 'Лосьон для ухода за кожей',
        93397: 'Духи'
    }
    """

    prod['cat_name'] = prod['type_id'].replace(cat_dic)
    cat_table = prod[['sku', 'description_category_id', 'type_id', 'cat_name']]
    
    
    if len(cat_table) > 0:
        #execute_query(connection, """DROP TABLE oz_prod_cats""")
        #create_sql_table('oz_prod_cats', cat_table, 'sku')
        #execute_query(connection, """ALTER TABLE oz_prod_cats ALTER COLUMN sku TYPE BIGINT""")

        execute_query(connection, """TRUNCATE TABLE oz_prod_cats""", p=False)
        export_to_sql('oz_prod_cats', cat_table)
        
        #logg(f"Обновляю категоии в таблице товаров", 3)
        #execute_query(connection, """
        #    UPDATE
        #        oz_products
        #    SET
        #        cat_name = oz_prod_cats.cat_name
        #    FROM
        #        oz_prod_cats
        #    WHERE
        #        oz_products.sku= oz_prod_cats.sku
        #    """, p=False)
        logg('------ Обновление категорий OZ завершено ------', 2)    
        
    return cat_table


def etl_oz_all():
    """
    Обновление всех таблиц OZ за раз, с "одной кнопки"
    Последовательно выполняются процедуры, описанные выше
    """  
    
    logg(f'------------- Начинаю обновление всех таблиц OZ -------------- ', 2)  

    try:
        etl_oz_products()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении товаров: {e}')

    try:
        etl_oz_postings()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении отправлений: {e}')

    try:
        etl_oz_stocks()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении остатков на складах FBO: {e}')

    try:
        etl_oz_returns()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении возвратов: {e}')

    try:
        etl_oz_prod_actions()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении товаров в акциях: {e}')

    try:
        elt_oz_discounts()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении запросов на скидку: {e}')

    try:
        elt_oz_transaction()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении транзакций: {e}')

    try:
        etl_oz_category()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении категорий: {e}')

    try:
        etl_currency() 
    except Exception as e:
        logg(f'!!! Ошибка при обновлении курсов валют: {e}')       
        
    logg(f'------------ Обновление всех таблиц OZ завершено ------------ ', 2)
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
    
    print(f'*** Старт скрипта sher_ozapi_to_sql с параметром {command} в {datetime.now()} ***') 
    
    command = command + '   '
    cc = command.split(' ')
    func = cc[0]
    fromdate = cc[1]

    #connection = None
    i = 0
    while i < 5:
        i +=1
        try:
            init_connection()
            base_reset()
            i = 10
        except:
            print(f'Попытка {i} установить соединение с базой SQL провалилась')
            time.sleep(3)

    try:
        connection
    except:
        print('Нет соединения с SQL базой!')
    else:
        logg(f'*** Команда для sher_ozapi_to_sql - [{command}] ***', 1)         
        match func:
            case 'all': etl_oz_all()
            case 'products': etl_oz_products()
            case 'postings': etl_oz_postings(fromdate)
            case 'stocks': etl_oz_stocks()
            case 'returns': etl_oz_returns(fromdate)
            case 'prod_actions': etl_oz_prod_actions()
            case 'discounts': elt_oz_discounts()  
            case 'transaction': elt_oz_transaction(fromdate)  
            case 'category': etl_oz_category()                                    
            case 'currency': etl_currency()
            case _: 
                logg('Не задан параметр!') 
        logg(f'*** Выполнена команда - [{command}] ***', 1) 
        connection.close()
    
         

    print(f'*** Завершение работы скрипта sher_ozapi_to_sql в {datetime.now()} ***') 

    return