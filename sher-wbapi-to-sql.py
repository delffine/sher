import requests
import pandas as pd
import psycopg2
import numpy as np
from datetime import date, timedelta, datetime
import hashlib
import sys
import time


# ------------------------------------------ 
#       Вспомогательные процедуры
# ------------------------------------------


#инициализация соединения с базой
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
        print(f"Create connection error: {e}")
    return connection

#запрос с ответом "ОК / не ОК"
#p=False - вообще без ответа
def execute_query(connection, query, p=True):
    connection.autocommit = True
    cursor = connection.cursor()    
    try:
        cursor.execute(query)
        if log_lev >= 3: 
            if p: logg(f'Query executed successfully {query[0:20]}...')
        result = True    
    except psycopg2.OperationalError as e:
        logg(f'Execute query error: {e}')
        result = False
    return result

#запрос с выводом данных в табличной форме
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
        logg(f"Read query error: {e}")
    return 

#лекарство от зависших ссессий
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
                    istr += "'" + str(tt.iloc[i][c]).replace("'", "") + "', "
                else:
                    nstr = str(tt.iloc[i][c])
                    if nstr == 'nan':
                        istr += "'NaN', "
                    else:
                        istr += nstr + ', '
                    #istr += str(tt.iloc[i][c]) + ', '
            istr = istr[:-2]
            istr += '), '+"\r\n"
            insert_row += istr
        insert_row = insert_row[:-4]
        insert_row += ' ON CONFLICT DO NOTHING ;'
        execute_query(connection, insert_row, False) 
        #if (len(data) > 1000) and (l % 5) == 0: 
        #    logg(f'Экспортировано в sql базу строк {l*inrows + len(tt)}', 3)
    if p:
        logg(f'Всего экспортировано в sql базу строк {l*inrows + len(tt)}', 3)    
    return

def generate_md5_hash(input_string):
    """
    Процедура расчета хэша входной строки по алгоритму md5
    Используется в таблицах, где нет одной уникальной колонки
    """
    md5_hash = hashlib.md5()
    md5_hash.update(input_string.encode('utf-8'))
    return md5_hash.hexdigest()    
    
def get_wb(url, head, params):
    """
    Процедура запроса WB по API
    На входе адрес запроса, ключи аутентификации и параметры
    Возвращает ответный датафрейм, в названия колонок приведены к нижнему регистру
    и добавлена колонка партнера. 
    Ответный датафрейм, скорее всего, потребудет дополнительной обработки
    в зависимости от запроса.    
    """
    res = pd.DataFrame([])
    logg(f'----- Запрос WB по url /{url.split("/")[-1]}, с параметрами {params} -----', 3)
    #перебираем партнеров
    for h in head:
        headers = {"Authorization": head[h]}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            dd = pd.DataFrame(response.json())
            #добавляем в табилцу партнера
            dd['partner'] = h
            #чтобы не брать пустые API ответы
            if len(dd) > 0:
                res = pd.concat([res, dd], ignore_index = True)
            logg(f'Получил {len(dd)} строк данных по {h}', 3)
        else:
            logg(f'Ошибка получения данных по API')
            logg(response.json())
    #приводим колонки к нижему регистру, чтобы все было однообразно
    res.columns = map(str.lower, res.columns)
    return res

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

    head={}
    head['basir'] = db_set['head_b']
    head['artur'] = db_set['head_a']
    head['kseniya'] = db_set['head_k']

    connection = create_connection(db_set['dbname'], db_set['uname'], db_set['pass'], db_set['server'], db_set['port'])
        
    db_set = dict()
    return

def logg(st='', lev=0):
    st = st.replace("'", "")
    if lev <= log_lev: print(f'{st}')
    sql = f"""INSERT INTO log (date, script, mess, log_lev) VALUES ('{datetime.now()}', 'wb', '{st}', {lev})"""
    execute_query(connection, sql, False)
    return

# ------------------------------------------ 
#              ELT скрипты
# ------------------------------------------


def etl_wb_income(fromdate=''):
    """
    Логика поставок выкупов по API
    - получение поставок по API с заданой даты (fromdate) или с 2024-01-01 (т.е. все что есть)
    - так как записей мало и нет уникального индификатора, то
    - просто чистим таблицу и в пустую таблиц записываем все полученные данные
    """      
    logg(f'-------- Начинаю обновление постановок ------', 2)
    if fromdate == '':    
        #now = date.today()
        #delta_days = 7
        #datefrom = str(now - timedelta(days=delta_days))
        #dateto = str(now)
        datefrom = '2024-01-01'
        
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/incomes"
    params = {"dateFrom": datefrom}  
    incomes = get_wb(url, head, params)

    if len(incomes) > 0:
        #удаляем одинарную кавычку, чтобы не морочится с экранизацией
        incomes['supplierarticle'] = incomes['supplierarticle'].str.replace("'", "")

        #Отбираем конкретные колонки для экспорта в SQL, так как WB периодически добавляет новые
        incomes = incomes[['incomeid', 'number', 'date', 'lastchangedate', 'supplierarticle',
               'techsize', 'barcode', 'quantity', 'totalprice', 'dateclose',
                'warehousename', 'nmid', 'status', 'partner']]

        logg(f'-- Надо перезаписать {len(incomes)} записей в SQL', 3)            
        execute_query(connection, """TRUNCATE TABLE wb_incomes""")
        export_to_sql('wb_incomes', incomes)
    else:
        logg('-- Нет данных для обновления SQL', 3)
        
    logg('--------- Обновление поставок завершено! ------- ', 2)    
    return

def etl_wb_orders(fromdate=''):
    """
    Логика обновления заказов по API
    - получение заказов с заданой (fromdate) или максимальной даты по API
    - определение srid, которые уже есть в SQL
    - определение srid, которые надо обновить и их обновление
    - экспорт новых данных, которых нет в SQL
    - при добавлении в SQL запишутся только строчки с srid, которых еще нет в базе
    - WB хранить заказы за посдение 6 месяцев
    """    
    logg('------ Начинаю обновление заказов ------', 2)
    if fromdate == '':
        fromdate = execute_read_query(connection, f"""SELECT MAX(lastchangedate) FROM wb_orders""")['max'][0][0:10]
        logg(f'Загрузка с максимальной даты в SQL {fromdate}', 3)
    else:
        logg(f'Загрузка с заданной даты {fromdate}', 3)

    url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    params = {"dateFrom": fromdate}
    orders = get_wb(url, head, params)
    #удаляем одинарную кавычку, чтобы не морочится с экранизацией
    orders['supplierarticle'] = orders['supplierarticle'].str.replace("'", "")

    #Отбираем конкретные колонки для экспорта в SQL, так как WB периодически добавляет новые
    #Сортируем по дате, чтобы записи шли в хронологическом порядке, а не по магазитна    
    orders = orders[['date', 'lastchangedate', 'warehousename', 'warehousetype',
       'countryname', 'oblastokrugname', 'regionname', 'supplierarticle',
       'nmid', 'barcode', 'category', 'subject', 'brand', 'techsize',
       'incomeid', 'issupply', 'isrealization', 'totalprice',
       'discountpercent', 'spp', 'finishedprice', 'pricewithdisc', 'iscancel',
       'canceldate', 'sticker', 'gnumber', 'srid', 'partner']].sort_values(by='date').reset_index(drop=True)

    # --------------- Заказы, которые надо обновить ----------------
    logg('-- Определяю заказы из API, которые уже есть в SQL и требуют обновления', 3)
    orders_srid_api = "'" + "', '".join(sorted(orders['srid'])) + "'"
    sql_data = execute_read_query(connection, f"""SELECT srid, lastchangedate FROM wb_orders WHERE srid IN ({orders_srid_api})""")
    srid_in_sql = set(sql_data['srid'])
    srid_to_insert = set(orders['srid']) - srid_in_sql
    
    api_lastchange = orders.query('srid.isin(@srid_in_sql)')[['srid', 'lastchangedate']]
    lastchanges = api_lastchange.merge(sql_data, how='left', on='srid', suffixes=['_api', '_sql'])
    srid_to_update = set(lastchanges.query('lastchangedate_api != lastchangedate_sql')['srid'])
    
    dd = orders.query('srid.isin(@srid_to_update)')
    l = len(dd) 
    if l > 0 :
        logg(f'Надо обновить заказов {l}', 3)
        for i in dd.index:
            sql = """UPDATE wb_orders SET\n"""
            zap = ''
            for col in dd.columns:
                sql += f"""{zap}{col} = '{dd.loc[i, col]}'\n"""
                zap = ', '
            sql += f"""WHERE srid = '{dd.loc[i,'srid']}';\n"""            
            if execute_query(connection, sql, p=False):
                #print(f"{dd.loc[i,'srid']} обновлен                        ")
                t = True
            else:
                logg(f"Ошибка при обновлении {dd.loc[i,'srid']}")
    else:
        logg('Нет заказов для обновления!', 3)
        
    # --------------- Новые заказы ----------------
    logg('-- Определяю новые заказы из API, которых нет в SQL', 3)
    #if len(srid_to_insert) > 0 :
    # Экспортируем новые заказы в sql
    orders_new = orders.query('srid.isin(@srid_to_insert)')
    if len(orders_new) > 0:
        logg(f'Новых заказов {len(srid_to_insert)}', 3)
        export_to_sql('wb_orders', orders_new)
        
    else:
        logg('Нет новых заказов!', 3)
        
    logg('------- Обновлени заказов завершено! ------- ', 2)
    return        


def etl_wb_sales(fromdate=''):
    """
    Логика обновления выкупов по API
    - получение заказов с заданой (fromdate) или максимальной даты по API
    - определение saleid, которые уже есть в SQL
    - определение saleid, которые надо обновить и их обновление
    - экспорт новых данных, которых нет в SQL
    - при добавлении в SQL запишутся только строчки с saleid, которых еще нет в базе
    - WB хранит выкупы за посдение 6 месяцев
    """
    logg(f'------ Начинаю обновление выкупов ------', 2)
    if fromdate == '':
        fromdate = execute_read_query(connection, f"""SELECT MAX(lastchangedate) FROM wb_sales""")['max'][0][0:10]
        logg(f'Загрузка с максимальной даты в SQL {fromdate}', 3)
    else:
        logg(f'Загрузка с заданной даты {fromdate}', 3)
         
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
    params = {"dateFrom": fromdate}
    sales = get_wb(url, head, params)

    if len(sales) == 0 : 
        logg('!!! Не получил никаких данных по API. Завершение процедуры', 0)
        return

    #удаляем одинарную кавычку, чтобы не морочится с экранизацией
    sales['supplierarticle'] = sales['supplierarticle'].str.replace("'", "")

    #Отбираем конкретные колонки для экспорта в SQL, так как WB периодически добавляет новые
    #Сортируем по дате, чтобы записи шли в хронологическом порядке, а не по магазитна    
    sales = sales[['date', 'lastchangedate', 'warehousename', 'warehousetype',
       'countryname', 'oblastokrugname', 'regionname', 'supplierarticle',
       'nmid', 'barcode', 'category', 'subject', 'brand', 'techsize',
       'incomeid', 'issupply', 'isrealization', 'totalprice',
       'discountpercent', 'spp', 'paymentsaleamount', 'forpay',
       'finishedprice', 'pricewithdisc', 'saleid', 'sticker', 'gnumber',
       'srid', 'partner']].sort_values(by='date').reset_index(drop=True)

    # --------------- Выкупы, которые надо обновить ----------------
    logg('-- Определяю выкупы из API, которые уже есть в SQL и требуют обновления', 3)
    sales_saleid_api = "'" + "', '".join(sorted(sales['saleid'])) + "'"
    sql_data = execute_read_query(connection, f"""SELECT saleid, lastchangedate FROM wb_sales WHERE saleid IN ({sales_saleid_api})""")
    saleid_in_sql = set(sql_data['saleid'])
    saleid_to_insert = set(sales['saleid']) - saleid_in_sql
    
    api_lastchange = sales.query('saleid.isin(@saleid_in_sql)')[['saleid', 'lastchangedate']]
    lastchanges = api_lastchange.merge(sql_data, how='left', on='saleid', suffixes=['_api', '_sql'])
    saleid_to_update = set(lastchanges.query('lastchangedate_api != lastchangedate_sql')['saleid'])
    
    dd = sales.query('saleid.isin(@saleid_to_update)')
    l = len(dd) 
    if l > 0 :
        logg(f'Надо обновить выкупов {l}', 3)
        for i in dd.index:
            sql = """UPDATE wb_sales SET\n"""
            zap = ''
            for col in dd.columns:
                sql += f"""{zap}{col} = '{dd.loc[i, col]}'\n"""
                zap = ', '
            sql += f"""WHERE saleid = '{dd.loc[i,'saleid']}';\n"""            
            if execute_query(connection, sql, p=False):
                #print(f"{dd.loc[i,'saleid']} обновлен                        ")
                t = True
            else:
                logg(f"Ошибка при обновлении {dd.loc[i,'saleid']}")
    else:
        logg('Нет выкупов для обновления!', 3)
        
    # --------------- Новые выкупы ----------------
    logg('-- Определяю новые выкупы из API, которых нет в SQL', 3)
    if len(saleid_to_insert) > 0 :
        logg(f'Новых выкупов {len(saleid_to_insert)}', 3)
        # Экспортируем новые выкупы в sql
        sales_new = sales.query('saleid.isin(@saleid_to_insert)')
        export_to_sql('wb_sales', sales_new)
        
    else:
        logg('Нет новых выкупов!', 3)
        
    logg('------- Обновление выкупов завершено! ------- ', 2)
    return   


def etl_wb_stock(fromdate=''):
    """
    Логика получения остатков на складах по API
    - получение остатков по API с заданой даты (fromdate) или с 2024-01-01 (т.е. все что есть)
    - так как записей мало и нет уникального индификатора, то
    - просто чистим таблицу и в пустую таблицу записываем все полученные данные
    """  


    logg(f'-------- Начинаю обновление остатков на складах ------', 2)
    if fromdate == '':     
        datefrom = '2024-01-01'

    logg(f'-------- Откидываю текущие остатки на складах в архив ------', 3)
    execute_query(connection, """
        INSERT INTO
            wb_stocks_histr
            SELECT
                nmid
                , (NOW() - INTERVAL '1 day')::date as date
                , SUM(quantity) as quantity
                , SUM(inwaytoclient) as inwaytoclient
                , SUM(inwayfromclient) as inwayfromclient
                , SUM(quantityfull) as quantityfull
            FROM
                wb_stocks
            GROUP BY
                1
        ON CONFLICT DO NOTHING;       
    """)

    url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
    params = {"dateFrom": datefrom}  
    stocks = get_wb(url, head, params)
    #удаляем одинарную кавычку, чтобы не морочится с экранизацией
    stocks['supplierarticle'] = stocks['supplierarticle'].str.replace("'", "")

    #Отбираем конкретные колонки для экспорта в SQL, так как WB периодически добавляет новые
    stocks = stocks[['lastchangedate', 'warehousename', 'supplierarticle', 'nmid', 'barcode',
       'quantity', 'inwaytoclient', 'inwayfromclient', 'quantityfull',
       'category', 'subject', 'brand', 'techsize', 'price', 'discount',
       'issupply', 'isrealization', 'sccode', 'partner']]

    if len(stocks) > 0:
        logg(f'-- Надо перезаписать {len(stocks)} записей в SQL', 3)        
        execute_query(connection, """TRUNCATE TABLE wb_stocks""")
        export_to_sql('wb_stocks', stocks)
    else:
        logg('Нет данных для обновления SQL', 3)
        
    logg('--------- Обновление остатков на складах завершено! -------', 2)    

    return    


def etl_wb_cards():
    """
    Логика получения карточек по API
    - получение карточек по API всех, что есть, но по 100 за запос, поэтому запросы иду в цикле
    - во всех тесктовых полях удаляется единичная кавычка
    - запрос в SQL, какие есть nmid карточек с датами обновления
    - карточки, которых по nmid нет в SQL - добалются в базу
    - карточки, которые есть в SQL и отличается дата обновления - обновляются целиком
    """       
    logg(f'------ Начинаю обновление карточек  ------', 2)
  
    url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    logg(f'----- Запрос WB по url /{url.split("/")[-1]} -----', 3)
    cards = pd.DataFrame([])

    for h in head:
        headers = {"Authorization": head[h]} 
        data = {"settings": {"cursor": {"limit": 100}, "filter": {"withPhoto": -1}}}
        total = 100
        logg(f'Старт получения карточек по {h}', 3)

        while total == 100:
            response = requests.post(url, headers=headers, json=data)  
            if response.status_code == 200:
                dd = pd.DataFrame(pd.DataFrame(response.json()['cards']))
                if len(dd) > 0:
                    dd['partner'] = h
                    cards = pd.concat([cards, dd], ignore_index = True)

                    updatedAt = response.json()['cursor']['updatedAt']
                    nmID = response.json()['cursor']['nmID']
                    total = response.json()['cursor']['total']
                    data = {"settings": {"cursor": {"updatedAt": updatedAt, "nmID": nmID, "limit": 100}, "filter": {"withPhoto": -1}}}

                    #logg(f'Получил еще {total} карточек по {h}. Всего загружено {len(cards), 3}               ')
                    
                else:
                    break
            else:
                logg(f'Ошибка получения карточек')
                logg(response.json())

    #удаляем кавычку, так как она не дружит с SQL
    cards.columns = map(str.lower, cards.columns)
    cards['vendorcode'] = cards['vendorcode'].str.replace("'", "")
    cards['description'] = cards['description'].str.replace("'", "")
    cards['photos'] = cards['photos'].str.replace("'", "")
    cards['video'] = cards['video'].str.replace("'", "")
    cards['dimensions'] = cards['dimensions'].str.replace("'", "")    
    cards['characteristics'] = cards['characteristics'].str.replace("'", "")    
    cards['sizes'] = cards['sizes'].str.replace("'", "")  
    cards['tags'] = cards['tags'].str.replace("'", "")
    cards['title'] = cards['title'].str.replace("'", "")      

    #Отбираем конкретные колонки для экспорта в SQL, так как WB периодически добавляет новые
    cards = cards[['nmid', 'imtid', 'nmuuid', 'subjectid', 'subjectname', 'vendorcode',
       'brand', 'title', 'description', 'video', 'needkiz', 'photos',
       'dimensions', 'characteristics', 'sizes', 'createdat', 'updatedat',
       'partner', 'tags']]
    
    # --------------- карточки, которые надо обновить ----------------
    logg('-- Определяю карточки из API, которые уже есть в SQL и требуют обновления', 3)
    #card_nmid_api = "'" + "', '".join(sorted(cards['nmid'])) + "'"
    sql_data = execute_read_query(connection, f"""SELECT nmid, updatedat FROM wb_cards""")
    sql_data['nmid'] = sql_data['nmid'].astype('int')
    cards_in_sql = set(sql_data['nmid'])
    cards_to_insert = set(cards['nmid']) - cards_in_sql
    
    api_updatedat = cards.query('nmid.isin(@cards_in_sql)')[['nmid', 'updatedat']]
    updatedat = api_updatedat.merge(sql_data, how='left', on='nmid', suffixes=['_api', '_sql'])
    cards_to_update = set(updatedat.query('updatedat_api != updatedat_sql')['nmid'])
    
    dd = cards.query('nmid.isin(@cards_to_update)')
    l = len(dd) 
    if l > 0 :
        logg(f'Надо обновить карточек {l}', 3)
        for i in dd.index:
            sql = """UPDATE wb_cards SET\n"""
            zap = ''
            for col in dd.columns:
                sql += f"""{zap}{col} = '{dd.loc[i, col]}'\n"""
                zap = ', '
            sql += f"""WHERE nmid = '{dd.loc[i,'nmid']}';\n"""            
            if execute_query(connection, sql, p=False):
                #print(f"\r{dd.loc[i,'nmid']} обновлен                        ")
                t = True
            else:
                logg(f"Ошибка при обновлении {dd.loc[i,'nmid']}", 3)
    else:
        logg('Нет карточек для обновления!', 3)
        
    # --------------- Новые карточки ----------------
    logg('-- Определяю новые карточки из API, которых нет в SQL', 3)
    if len(cards_to_insert) > 0 :
        logg(f'Новых карточек {len(cards_to_insert)}', 3)
        # Экспортируем новые карточки в sql
        cards_new = cards.query('nmid.isin(@cards_to_insert)')
        export_to_sql('wb_cards', cards_new)
        
    else:
        logg('Нет новых карточек!', 3)

    logg('---- Обновление карточек завершено! ------ ', 2)    
    return


def etl_wb_goodsreturn(fromdate=''):
    """
    Логика получения возвратов по API
    - получение возвратов по API с заданой даты или с сегодня и за последние 14 дней
    - так как записей мало и нет уникального индификатора, то
    - просто чистим таблицу и в пустую таблицу записываем все полученные данные
    """          
    logg(f'------ Начинаю обновление возвратов  ------', 2)

    
    if fromdate == '': 
        now = date.today()
    else:
        now = fromdate
        
    delta_days = 14
    datefrom = str(now - timedelta(days=delta_days))
    dateto = str(now)
    url = "https://seller-analytics-api.wildberries.ru/api/v1/analytics/goods-return"
    params = {"dateFrom": datefrom, "dateTo": dateto}  
    goodsreturn_j = get_wb(url, head, params)
    if len(goodsreturn_j) > 0:
        goodsreturn = pd.json_normalize(goodsreturn_j['report'])
        goodsreturn['partner'] = goodsreturn_j['partner']
        goodsreturn.columns = goodsreturn.columns.str.lower()
    if len(goodsreturn) > 0 :

        #Отбираем конкретные колонки для экспорта в SQL, так как WB периодически добавляет новые
        goodsreturn = goodsreturn[['barcode', 'brand', 'dstofficeaddress', 'dstofficeid', 'isstatusactive',
            'nmid', 'orderdt', 'returntype', 'shkid', 'srid', 'status', 'stickerid',
            'subjectname', 'techsize', 'partner', 'completeddt', 'expireddt',
            'readytoreturndt', 'reason', 'orderid']]

        logg(f'-- Надо экспортировать {len(goodsreturn)} записей в SQL', 3)
        #надо ли очишщать таблицу перед записью новых?
        #записи апдейтятся ? 
        #execute_query(connection, """TRUNCATE TABLE wb_goodsreturn""")
        export_to_sql('wb_goodsreturn', goodsreturn)
    else:
        logg('-- Нет данных для обновления SQL', 3)
        
    logg('---- Обновление возвратов завершено! ------', 2)       
    return

def etl_wb_adv_expenses(fromdate=''):
    """
    Логика получения трат рекламных кампаний по API
    - запрос по SQL самой последений даты обновлений в таблице трат
    - получение возвратов по API с максимальной в SQL даты или с заданной и за последние 7 дней
    - формирование хэша по колонкам updtime + partner + advertid + updsum
    - сравнивааем данные, полученные по API, на обновление по колонкам updnum, updsum, advertstatus
    - совсем новые строчки просто добавляются в таблицу
    - строчки с изменившиися показателями обновляются в SQL по хэшу как ключу        
    """          
    
    logg(f'------ Начинаю обновление трат рекламных кампаний  ------', 2)
    
    if fromdate == '':
        datefrom = execute_read_query(connection, f"""SELECT MAX(updtime) FROM wb_adv_expenses""")['max'][0][0:10]
        datefrom = datetime.strptime(datefrom, '%Y-%m-%d').date()
        dateto = date.today()
        if (dateto - datefrom).days > 30:
            delta_days = 30
            dateto = datefrom + timedelta(days=delta_days)
    else:
        datefrom = datetime.strptime(fromdate, '%Y-%m-%d').date()
        delta_days = 30
        dateto = datefrom + timedelta(days=delta_days)
        if dateto > date.today():
            dateto = date.today()

    datefrom = str(datefrom)    
    dateto = str(dateto)

    
    url = f"https://advert-api.wildberries.ru/adv/v1/upd?from={datefrom}&to={dateto}"
    params = {"dateFrom": datefrom, "dateTo": dateto}  
    adv_expenses = get_wb(url, head, params)

    #удаляем одинарную кавычку, чтобы не морочится с экранизацией
    adv_expenses['campname'] = adv_expenses['campname'].str.replace("'", "")
    
    #добавляем в качестве ключевого поля хэш по колонкам updtime + partner + advertid + updsum
    adv_expenses['hash'] = adv_expenses.apply(lambda x: 
            generate_md5_hash(f"{x['updtime']}{x['partner']}{x['advertid']}{x['updsum']}"), axis=1)

    #Отбираем конкретные колонки для экспорта в SQL, так как WB периодически добавляет новые
    adv_expenses = adv_expenses[['updtime', 'campname', 'paymenttype', 'updnum', 'updsum', 'advertid',
       'adverttype', 'advertstatus', 'partner', 'hash']]

    insql = execute_read_query(connection, f"""SELECT updnum, updsum, advertstatus, hash FROM wb_adv_expenses""")

    
    if len(insql) > 0:
        logg('Определяем данные для обновления', 3)
        insql['updnum'] = insql['updnum'].astype('int64')
        insql['updsum'] = insql['updsum'].astype('float') 
        insql['advertstatus'] = insql['advertstatus'].astype('int64')     
        compare = adv_expenses.merge(insql, 
                   how = 'left', 
                   left_on = ['hash'],
                   right_on = ['hash'],
                   suffixes = ['_n', '_old']
                  )
        toupdate = compare.query('(updnum_n != updnum_old or updsum_n != updsum_old or advertstatus_n != advertstatus_old) and ~updsum_old.isna()')
        toupdate = toupdate.rename(columns={'updnum_n': 'updnum', 'updsum_n': 'updsum', 'advertstatus_n': 'advertstatus'}).\
                    drop(['updnum_old', 'updsum_old', 'advertstatus_old'], axis=1)

        toinsert = compare.query('updsum_old.isna()')
        toinsert = toinsert.rename(columns={'updnum_n': 'updnum', 'updsum_n': 'updsum', 'advertstatus_n': 'advertstatus'}).\
                drop(['updnum_old', 'updsum_old', 'advertstatus_old'], axis=1)    

        if len(toinsert) > 0:
            logg(f'--- Добавляем новые данные {len(toinsert)}', 3)

            export_to_sql('wb_adv_expenses', toinsert)
        else:
            logg('Нет новых строк для добавления!', 3)

        if len(toupdate) > 0:
            logg(f'--- Строк для обновления колонок {len(toupdate)}', 3)
            for i in toupdate.index:
                #print(f"\rОбновляю {toupdate.loc[i,'hash']}                        ", end='')
                sql = """UPDATE wb_adv_expenses SET\n"""
                zap = ''
                for col in toupdate.columns:

                    upd_val = toupdate.loc[i, col]
                    if upd_val == 'nan' or upd_val == 'NaN' or upd_val == 'None':
                        upd_val = 'NULL'
                    else:
                        upd_val = f"'{upd_val}'"
                    sql += f"""{zap}{col} = {upd_val}\n"""
                    zap = ', '
                sql += f"""WHERE hash = '{toupdate.loc[i,'hash']}';\n"""            
                execute_query(connection, sql, p=False)  
            logg(f'\n--- Колонки обновлены ---- ', 3)        
        else:
            logg('Нет строк для обновления данных!', 3)

    else:
        logg('--- В базе ничего нет, просто вставляем данные в SQL', 3)
        export_to_sql('wb_adv_expenses', adv_expenses)


    logg('------- Обновление трат рекламных компаний завершено! -------\n', 2)

    return


def elt_wb_adv_stat(fromdate=''):
    """
    Логика получения рекламной статистики по API
    - запрос по SQL самой последений даты обновлений в таблице рекламной статистики
    - получение даннвх по API с максимальной в SQL или даты, заданной входным переметровм, и за последние 7 дней
    - распаковка трехуровнего Json в Jsonе: даты -> платформы ->артикулы
    - перевод в нижний регистр названия колонок, удаление единичной кавычки
    - формирование хэша по колонкам date + partner + advertid + nmid + apptype
    - сравнивааем данные, полученные по API, на обновление по колонкам views, sum, sum_price
    - совсем новые строчки просто добавляются в таблицу
    - строчки с изменившиися показателями обновляются в SQL по хэшу как ключу    
    """       
    logg('------ Начинаю обновление рекламной статистики ------ ', 2)

    adv_stat = pd.DataFrame([])

    if fromdate == '':
        datefrom = execute_read_query(connection, f"""SELECT MAX(date) FROM wb_adv_stat""")['max'][0][0:10]
        datefrom = datetime.strptime(datefrom, '%Y-%m-%d').date() - timedelta(days=14)
        dateto = date.today()
        if (dateto - datefrom).days > 30:
            delta_days = 30
            dateto = datefrom + timedelta(days=delta_days)
    else:
        datefrom = datetime.strptime(fromdate, '%Y-%m-%d').date()
        delta_days = 30
        dateto = datefrom + timedelta(days=delta_days)
        if dateto > date.today():
            dateto = date.today()

        datefrom = str(datefrom)    
        dateto = str(dateto)

    all_adverts = execute_read_query(connection, f"""
        SELECT 
            advertid
            , partner
        FROM
            wb_adv_expenses
        WHERE
            updtime::date BETWEEN '{datefrom}'::date AND '{dateto}'::date
        GROUP BY 
            1, 2
        """)

    for h in head:
        advertids = list(all_adverts.query('partner == @h')['advertid'])
        if len(advertids) > 0:
            requst_data = list()
            for a in advertids:
                requst_data.append({"id": int(a), "interval": {"begin":str(datefrom), "end":str(dateto)}})

            url = "https://advert-api.wildberries.ru/adv/v2/fullstats"
            headers = {"Authorization": head[h]}
            response = requests.post(url, headers=headers, json=requst_data)  
            if response.status_code == 200:
                dd = pd.json_normalize(response.json())
                #print('Распаковываю Json в Jsonе')
                days_t = dd[['advertId', 'days']].explode(['days']).reset_index()
                days = pd.json_normalize(days_t['days'])
                days['advertId'] = days_t['advertId']
                apps_t = days[['advertId', 'date', 'apps']].explode('apps').reset_index(drop=True)
                apps = pd.json_normalize(apps_t['apps'])
                apps[['advertId', 'date']] = apps_t[['advertId', 'date']]
                art_t = apps[['nm', 'appType', 'advertId', 'date']].explode('nm').reset_index(drop=True)
                art = pd.json_normalize(art_t['nm'])
                art[['advertId', 'date', 'appType']] = art_t[['advertId', 'date', 'appType']]
                art['partner'] = h
                art = art.dropna()
                logg(f'Получил еще {len(art)} строк рекламной статистики партнера {h} с {datefrom} по {dateto}', 3)
                if len(art) > 0:
                    adv_stat = pd.concat([adv_stat, art], ignore_index = True)

            else:
                logg('Ошибка получения данных по API')
                logg(response.json())
        else:
            logg(f'Для партнера {h} нет рекламной стастики с {datefrom} по {dateto}', 3)

    #приводим колонки к нижему регистру, чтобы все было однообразно        
    adv_stat.columns = map(str.lower, adv_stat.columns)

    #удаляем одинарную кавычку, чтобы не морочится с экранизацией
    adv_stat['name'] = adv_stat['name'].str.replace("'", "")

    #добавляем в качестве ключевого поля хэш по колонкам date + partner + advertid + nmid + apptype
    adv_stat['hash'] = adv_stat.apply(lambda x: 
        generate_md5_hash(f"{x['date']}{x['partner']}{x['advertid']}{x['nmid']}{x['apptype']}"), axis=1)

    #Отбираем конкретные колонки для экспорта в SQL, так как WB периодически добавляет новые
    adv_stat = adv_stat[['views', 'clicks', 'ctr', 'cpc', 'sum', 'atbs', 'orders', 'cr', 'shks',
       'sum_price', 'name', 'nmid', 'advertid', 'date', 'apptype', 'partner',
       'hash']]

    insql = execute_read_query(connection, f"""SELECT views, sum, sum_price, hash FROM wb_adv_stat""")

    if len(insql) > 0:
        logg('Определяем данные для обновления', 3)
        insql['views'] = insql['views'].astype('float')
        insql['sum'] = insql['sum'].astype('float') 
        insql['sum_price'] = insql['sum_price'].astype('float')     
        compare = adv_stat.merge(insql, 
                   how = 'left', 
                   left_on = ['hash'],
                   right_on = ['hash'],
                   suffixes = ['_n', '_old']
                  )
        toupdate = compare.query('(views_n != views_old or sum_n != sum_old or sum_price_n != sum_price_old) and ~views_old.isna()')
        toupdate = toupdate.rename(columns={'views_n': 'views', 'sum_n': 'sum', 'sum_price_n': 'sum_price'}).\
                    drop(['views_old', 'sum_old', 'sum_price_old'], axis=1)

        toinsert = compare.query('views_old.isna()')
        toinsert = toinsert.rename(columns={'views_n': 'views', 'sum_n': 'sum', 'sum_price_n': 'sum_price'}).\
                drop(['views_old', 'sum_old', 'sum_price_old'], axis=1)    

        if len(toinsert) > 0:
            logg(f'--- Добавляем новые данные {len(toinsert)}', 3)

            export_to_sql('wb_adv_stat', toinsert)
        else:
            logg('Нет новых строк для добавления!', 3)

        if len(toupdate) > 0:
            logg(f'--- Строк для обновления колонок {len(toupdate)}', 3)
            for i in toupdate.index:

                sql = """UPDATE wb_adv_stat SET\n"""
                zap = ''
                for col in toupdate.columns:

                    upd_val = toupdate.loc[i, col]
                    if upd_val == 'nan' or upd_val == 'NaN' or upd_val == 'None':
                        upd_val = 'NULL'
                    else:
                        upd_val = f"'{upd_val}'"
                    sql += f"""{zap}{col} = {upd_val}\n"""
                    zap = ', '
                sql += f"""WHERE hash = '{toupdate.loc[i,'hash']}';\n"""            
                execute_query(connection, sql, p=False)
            logg(f'\n--- Колонки обновлены ---- ', 3)        
        else:
            logg('Нет строк для обновления данных!', 3)

    else:
        logg('--- В базе ничего нет, просто вставляем данные в SQL', 3)
        export_to_sql('wb_adv_stat', adv_stat)


    logg('------ Обновление рекламной статистики завершено ------ ', 2)
    return



def etl_wb_finreport(fromdate=''):
    """
    Логика получения финасового отчета по API
    - запрос по SQL самой последений даты обновлений в таблице финотчета
    - получение данных по API, начиная с максимальной в SQL или даты, заданной входным переметровм, и за последние 7 дней
    - добавление в таблицу всех полученных по API данных
    - при этом строчки с праймори ключом rrd_id, уже имеющиеся в SQL, не будут добавлены       
    """          
    logg(f'------ Начинаю обновление финансового отчета  ------', 2)
    
    if fromdate == '':
        datefrom = execute_read_query(connection, f"""SELECT MAX(rr_dt) FROM wb_finreport""")['max'][0][0:10]
        datefrom = datetime.strptime(datefrom, '%Y-%m-%d').date()
        dateto = date.today()
        if (dateto - datefrom).days > 30:
            delta_days = 30
            dateto = datefrom + timedelta(days=delta_days)
    else:
        datefrom = datetime.strptime(fromdate, '%Y-%m-%d').date()
        delta_days = 7
        dateto = datefrom + timedelta(days=delta_days)
        if dateto > date.today():
            dateto = date.today()

    datefrom = str(datefrom)    
    dateto = str(dateto)
    
    url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
    params = {"dateFrom": datefrom, "dateTo": dateto}  
    finreport = get_wb(url, head, params)

    #Отбираем конкретные колонки для экспорта в SQL, так как WB периодически добавляет новые
    finreport = finreport[['realizationreport_id', 'date_from', 'date_to', 'create_dt',
       'currency_name', 'suppliercontract_code', 'rrd_id', 'gi_id', 'dlv_prc',
       'fix_tariff_date_from', 'fix_tariff_date_to', 'subject_name', 'nm_id',
       'brand_name', 'sa_name', 'ts_name', 'barcode', 'doc_type_name',
       'quantity', 'retail_price', 'retail_amount', 'sale_percent',
       'commission_percent', 'office_name', 'supplier_oper_name', 'order_dt',
       'sale_dt', 'rr_dt', 'shk_id', 'retail_price_withdisc_rub',
       'delivery_amount', 'return_amount', 'delivery_rub', 'gi_box_type_name',
       'product_discount_for_report', 'supplier_promo', 'ppvz_spp_prc',
       'ppvz_kvw_prc_base', 'ppvz_kvw_prc', 'sup_rating_prc_up', 'is_kgvp_v2',
       'ppvz_sales_commission', 'ppvz_for_pay', 'ppvz_reward', 'acquiring_fee',
       'acquiring_percent', 'payment_processing', 'acquiring_bank', 'ppvz_vw',
       'ppvz_vw_nds', 'ppvz_office_name', 'ppvz_office_id', 'ppvz_supplier_id',
       'ppvz_supplier_name', 'ppvz_inn', 'declaration_number',
       'bonus_type_name', 'sticker_id', 'site_country', 'srv_dbs', 'penalty',
       'additional_payment', 'rebill_logistic_cost', 'storage_fee',
       'deduction', 'acceptance', 'assembly_id', 'srid', 'report_type',
       'is_legal_entity', 'trbx_id', 'installment_cofinancing_amount',
       'wibes_wb_discount_percent', 'rebill_logistic_org', 'kiz', 'partner',
       'cashback_amount', 'cashback_discount']].sort_values(by='create_dt').reset_index(drop=True)

    if len(finreport) > 0:
        logg(f'-- Надо экспортировать {len(finreport)} записей в SQL', 3)   
        export_to_sql('wb_finreport', finreport) 
    else:
        logg('-- Нет данных для обновления SQL', 3)
        
    logg('------- Обновление финасового отчета завершено! -------', 2)
    
    return


def etl_fbs_orders():
    url = "https://marketplace-api.wildberries.ru/api/v3/orders"
    res = pd.DataFrame([])

    logg('------ Начинаю обновление сборочных заданий ------', 2)
    logg(f'Запрос WB по url /{url.split("/")[-1]}', 3)
    for h in head:
        logg(f'Старт получения карточек по {h}', 3)     
        next_id = execute_read_query(connection, f"""SELECT MAX(id) FROM wb_fbs_orders WHERE partner = '{h}'""")['max'][0]
        if next_id:
            # Уменьшаем next_id чтобы получить данные с перекрестом
            next_id -= 10
        else:
            next_id = 0
        #next_id = 0 
        headers = {"Authorization": head[h]}
        while True:
            params = {'limit' : 1000, 'next' : next_id}
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                dd = pd.DataFrame(response.json())        
                if len(dd) > 0:
                    next_id = dd.iloc[-1]['next']
                    dd = pd.json_normalize(dd['orders'])
                    dd['partner'] = h
                    res = pd.concat([res, dd], ignore_index = True)
                    #print(f'Получил еще {len(dd)} заданий для {h} всего заданий {len(res)}, next {next_id}          ')                
                else:
                    logg(f"Для {h} получено заданий {len(res.query('partner==@h'))}", 3)                
                    break
            else:
                logg(f'Ошибка получения данных по API')
                logg(response.json())
                break
                
    res.columns = map(str.lower, res.columns)
    res = res.rename(columns={'options.isb2b' : 'options_isb2b'})

    #Отбираем конкретные колонки для экспорта в SQL, так как WB периодически добавляет новые
    res = res[['address', 'scanprice', 'comment', 'deliverytype', 'supplyid',
       'orderuid', 'article', 'colorcode', 'rid', 'createdat', 'offices',
       'skus', 'id', 'warehouseid', 'nmid', 'chrtid', 'price',
       'convertedprice', 'currencycode', 'convertedcurrencycode', 'cargotype',
       'iszeroorder', 'options_isb2b', 'partner', 'officeid']].sort_values(by='createdat').reset_index(drop=True)

    if len(res) > 0:
        logg(f'-- Надо экспортировать {len(res)} записей в SQL --', 3)
        res = res.fillna('nan')
        export_to_sql('wb_fbs_orders', res)

    logg('------ Обновление сборочных зданий завершено! ------', 2)
    
    return res

def wb_stocks_coords():
    #Щербинка 55.501315, 37.564554
    #Тула 54.193208, 37.617273
    #
    url = f"https://marketplace-api.wildberries.ru/api/v3/offices"
    params = {}
    h1 = {'basir' : head['basir']}
    stocks = get_wb(url, h1, params)
    #stocks = stocks.drop_duplicates('name').reset_index(drop=True)


    sql_stocks = execute_read_query(connection, f"""
    SELECT
        warehousename
        , count(warehousename)
    FROM
        wb_stocks
    GROUP BY
        warehousename
    --limit 10
    ORDER BY
        2 DESC
    """)

    for i in sql_stocks.index:
        wn = sql_stocks.loc[i, 'warehousename']
        wn = wn.replace('СЦ ', '')\
            .replace(' 1', '')\
            .replace(' 2', '')\
            .replace(' 3', '')
        wn = wn.split(' ')[0]
        sq = stocks.query('name.str.contains(@wn)')
        if len(sq) > 0:
            sql_stocks.loc[i, 'coord'] = f"[{sq.iloc[0]['latitude']},{sq.iloc[0]['longitude']}]"
            sql_stocks.loc[i, 'address'] = sq.iloc[0]['address']
            sql_stocks.loc[i, 'federaldistrict'] = sq.iloc[0]['federaldistrict']
            sql_stocks.loc[i, 'city'] = sq.iloc[0]['city']

    sql_stocks = sql_stocks.drop('count', axis = 1)        

    sql_stocks.loc[sql_stocks['warehousename'] == 'Щербинка',  'coord'] = '[55.501315,37.564554]'
    sql_stocks.loc[sql_stocks['warehousename'] == 'Щербинка',  'address'] = 'Щербинка'
    sql_stocks.loc[sql_stocks['warehousename'] == 'Щербинка',  'city'] = 'Щербинка'
    sql_stocks.loc[sql_stocks['warehousename'] == 'Щербинка',  'federaldistrict'] = 'Центральный федеральный округ'

    sql_stocks.loc[sql_stocks['warehousename'] == 'Тула', 'coord'] = '[54.193208,37.617273]'
    sql_stocks.loc[sql_stocks['warehousename'] == 'Тула', 'address'] = 'Тула'
    sql_stocks.loc[sql_stocks['warehousename'] == 'Тула', 'city'] = 'Тула'
    sql_stocks.loc[sql_stocks['warehousename'] == 'Тула', 'federaldistrict'] = 'Центральный федеральный округ'

    sql_stocks.loc[sql_stocks['warehousename'] == 'Электросталь', 'coord'] = '[55.784450,38.444858]'
    sql_stocks.loc[sql_stocks['warehousename'] == 'Электросталь', 'address'] = 'Электросталь'
    sql_stocks.loc[sql_stocks['warehousename'] == 'Электросталь', 'city'] = 'Электросталь'
    sql_stocks.loc[sql_stocks['warehousename'] == 'Электросталь', 'federaldistrict'] = 'Центральный федеральный округ'

    sql_stocks.loc[sql_stocks['warehousename'] == 'СЦ Шымкент', 'coord'] = '[42.315521,69.586942]'
    sql_stocks.loc[sql_stocks['warehousename'] == 'СЦ Шымкент',  'address'] = 'Шымкент'
    sql_stocks.loc[sql_stocks['warehousename'] == 'СЦ Шымкент',  'city'] = 'Шымкент'
    sql_stocks.loc[sql_stocks['warehousename'] == 'СЦ Шымкент',  'federaldistrict'] = 'Зарубеж'

    sql_stocks.loc[sql_stocks['warehousename'] == 'СЦ Тбилиси 3 Бонд', 'coord'] = '[41.693083,44.801561]'
    sql_stocks.loc[sql_stocks['warehousename'] == 'СЦ Тбилиси 3 Бонд',  'address'] = 'Тбилиси'
    sql_stocks.loc[sql_stocks['warehousename'] == 'СЦ Тбилиси 3 Бонд',  'city'] = 'Тбилиси'
    sql_stocks.loc[sql_stocks['warehousename'] == 'СЦ Тбилиси 3 Бонд',  'federaldistrict'] = 'Зарубеж'

    sql_stocks['federaldistrict'] = sql_stocks['federaldistrict'].fillna('Зарубеж')
        
    execute_query(connection, """DROP TABLE wb_stocks_coord""")
    create_sql_table('wb_stocks_coord', sql_stocks, '')
    execute_query(connection, """TRUNCATE TABLE wb_stocks_coord""")
    export_to_sql('wb_stocks_coord', sql_stocks)

    return sql_stocks


def etl_wb_feedback(fromdate=''):
    """
    Логика получения отзывов по товарам через API
    - запрос по SQL самой последений даты обновлений в таблице отзывов
    - получение отзывов возвратов по API с максимальной в SQL даты или с заданной и за последние 30 дней
        ограничение: 5000 отзывов за запрос. Обычно хватает на месяц отзывов, на два уже не всегда.
    - распаковка полученных данных, данные о товаре json-в-json
    - в дальшую работу берем только нужные колоноки
    - экспорт в таблицу полученных по API данных
    - при этом строчки с имеющимися в SQL id отзыва не будут добавлены
    
    """
    logg(f'------ Начинаю обновление отзывов  ------', 2)  

    if fromdate == '':
        datefrom = execute_read_query(connection, f"""SELECT MAX(date) FROM wb_feedback""")['max'][0][0:10]
        datefrom = datetime.strptime(datefrom, '%Y-%m-%d').date()
        dateto = date.today()
        if (dateto - datefrom).days > 30:
            delta_days = 30
            dateto = datefrom + timedelta(days=delta_days)
    else:
        datefrom = datetime.strptime(fromdate, '%Y-%m-%d').date()
        delta_days = 30
        dateto = datefrom + timedelta(days=delta_days)
        if dateto > date.today():
            dateto = date.today()

    udatefrom = int(pd.to_datetime(str(datefrom)).timestamp())
    udateto = int(pd.to_datetime(str(dateto)).timestamp())
    

    url = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"
    params = {'isAnswered' : True,
                'take' : 5000,
                'skip' : 0,
                'dateFrom' : udatefrom,
                'dateTo' : udateto
                 }
    result = pd.DataFrame([])
    feedback = pd.DataFrame([])
    for h in head:
        headers = {"Authorization": head[h]}
        logg(f'--- Запрашиваю отзывы по товарам партнера {h} c {str(datefrom)} по {str(dateto)}', 3)
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            dd = pd.DataFrame(response.json()['data']['feedbacks'])
            dd2 = pd.concat([dd.drop(['productDetails'], axis=1), 
                    pd.json_normalize(dd['productDetails']).add_prefix('prod_'), 
                            ], axis=1)

            result = dd2[['id', 'text', 'pros', 'cons', 'productValuation', 'createdDate', 
                            'prod_nmId', 'prod_supplierArticle', 'prod_imtId']].copy()


            result.columns = ('id', 'text', 'pros', 'cons', 'rating', 'date', 'nmid', 'supplierarticle', 'imtid')    
            result['partner'] = h
            result['date'] = pd.to_datetime(result['date']).dt.date
            logg(f'Получил {len(result)} отзывов по товарам партнера {h}', 3)

            feedback = pd.concat([feedback, result])

        else:
            logg(f'Ошибка получения данных')
            #print(response.json())


    feedback = feedback.reset_index(drop = True)

    if len(feedback) > 0:
        logg(f'Надо экспортировать в SQL {len(feedback)} отзывов', 3)

        #execute_query(connection, """DROP TABLE wb_feedback""")
        #create_sql_table('wb_feedback', feedback, '')
        #execute_query(connection, """ALTER TABLE wb_feedback
        #  ADD CONSTRAINT feedback_id
        #    PRIMARY KEY (id)""")
        #execute_query(connection, """ALTER TABLE wb_feedback 
        #    ALTER COLUMN nmid TYPE BIGINT""")
        #execute_query(connection, """ALTER TABLE wb_feedback 
        #    ALTER COLUMN imtid TYPE BIGINT""")    
        #execute_query(connection, """TRUNCATE TABLE wb_feedback""")

        #print(f'-- Экспортирую {len(feedback)} строк отзывов')    
        export_to_sql('wb_feedback', feedback)    

    else:
        logg('Нет данных для экспорта')

    logg(f'------ Обновление отзывов завершено! ------', 2)  

    return


def etl_wb_voronka(fromdate=''):
    """
    Данные в WB API могут меняться для прошлых дат. 
    Похоже, что заказы, выкупы и отмены считаются в связке с датой первого события "добавление в корзину"
    Поэтому надо обновлять данные воронки за прошлые даты!
    ---
    Логика получения ворнки продаж через API
    - запрос по SQL самой последений даты обновлений в таблице вороки продаж
    - получение отзывов возвратов по API с максимальной в SQL даты или с заданной и за последующие 30 дней
    - в цикле запрос по дням
        - в цикле запрос по партерам
            - в цикле запрос по одной станице, в каждой по 1000 записей
                ограниечние: один запрос в 20 сек, поэтому делаем паузу после запроса
              распоковув ответа json-в-json
              в дальнейщую работу идут только нужные колонки
        - предобработка данных
        - удаляются старые данные на дату  
        - экспорт данных в SQL за один день по всем партнерам
                
    """
    logg(f'------ Начинаю обновление воронки продаж  ------', 2)  

    url = f"https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail"
    voronka = pd.DataFrame([])

    delta_days = 14

    if fromdate == '':
        #datefrom = execute_read_query(connection, f"""SELECT MAX(date) FROM wb_voronka""")['max'][0][0:10]
        #datefrom = datetime.strptime(datefrom, '%Y-%m-%d').date() - timedelta(days=delta_days)
        #dateto = date.today()
        #if (dateto - datefrom).days > delta_days:
        #    dateto = datefrom + timedelta(days=delta_days)
        #По умолчанию запрашиваем воронку за последние 14 дней 
        datefrom = date.today() - timedelta(days=delta_days)
        dateto = date.today() - timedelta(days=1)
    else:
        datefrom = datetime.strptime(fromdate, '%Y-%m-%d').date()
        dateto = datefrom + timedelta(days=delta_days)
        if dateto > date.today():
            dateto = date.today()

    datefrom = str(datefrom)    
    dateto = str(dateto)

    logg(f'----- Экспорт воронки продаж WB с {datefrom} по {dateto} -----', 3) 
        
    start_date = datetime.strptime(datefrom, '%Y-%m-%d').date()
    end_date = datetime.strptime(dateto, '%Y-%m-%d').date()

    delta = timedelta(days=1)  
    current_date = start_date  
    while current_date <= end_date:  
        logg(f'--- Начинаю запрос на дату {current_date}', 2) 
        voronka_oneday = pd.DataFrame([])    
        for h in head:
            logg(f'Начинаю запрос для партрера {h}', 3)      
            headers = {"Authorization": head[h]} 
            i = 1
            voronka_onepartner = pd.DataFrame([])
            while True:
                params = {
                    "period": {
                        'begin': str(current_date) + ' 00:00:00',
                        'end': str(current_date) + ' 23:59:59',    
                    },
                    'page' : i
                }

                response = requests.post(url, headers=headers, json=params) 
                if response.status_code == 200:
                    dd = pd.DataFrame(response.json()['data']['cards'])
                    if len(dd) > 0:
                        dd2 = pd.concat([dd.drop(['object', 'statistics', 'stocks'], axis=1), 
                                pd.json_normalize(dd['object']).add_prefix('obj_'), 
                                pd.json_normalize(dd['statistics']).add_prefix('stat_'),
                                        ], axis=1)

                        voronka_onepartner = pd.concat([voronka_onepartner, 
                            dd2[['nmID', 'vendorCode', 'brandName', 'obj_name', 'stat_selectedPeriod.begin', 'stat_selectedPeriod.end' 
                              , 'stat_selectedPeriod.openCardCount', 'stat_selectedPeriod.addToCartCount', 'stat_selectedPeriod.ordersCount'
                              , 'stat_selectedPeriod.ordersSumRub', 'stat_selectedPeriod.buyoutsCount', 'stat_selectedPeriod.buyoutsSumRub'
                              , 'stat_selectedPeriod.cancelCount', 'stat_selectedPeriod.cancelSumRub', 'stat_selectedPeriod.avgPriceRub'
                              , 'stat_selectedPeriod.avgOrdersCountPerDay']]
                                           ])
                        i += 1
                    #------ if len(dd)        
                        
                    if len(dd) < 1000:
                        break
                    else:
                        time.sleep(20)
                else:
                    if response.status_code == 429:
                        logg(f'Ошибка -Много запросов-. Ждем 10 сек....', 3)
                        time.sleep(10)
                    else:
                        logg(f'!!! Ошибка {response.status_code}. Break!!!')
                        break
   
                #----------- if response

            #----------- while True

            logg(f'Получил {len(voronka_onepartner)} строк для партрера {h} за дату {current_date}', 3)    
            voronka_onepartner['partner'] = h

            voronka_oneday = pd.concat([voronka_oneday, voronka_onepartner])        

        #----------- for h in head 

        logg(f'-- Всего получил строк {len(voronka_oneday)} на дату {current_date}', 3)     

        if len(voronka_oneday) > 0:
            logg(f'Предобрабокта данных перед экспортом в SQL', 3)    

            voronka_oneday.columns = map(str.lower, voronka_oneday.columns)
            voronka_oneday.columns = voronka_oneday.columns.str.replace('stat_selectedperiod.', '')
            voronka_oneday['begin'] = pd.to_datetime(voronka_oneday['begin']).dt.date            
            voronka_oneday = voronka_oneday.rename(columns = {'obj_name' : 'subject', 'begin' : 'date'})

            voronka_oneday['sss'] = voronka_oneday['opencardcount'] + voronka_oneday['addtocartcount'] + voronka_oneday['orderscount'] \
                    + voronka_oneday['buyoutscount'] + voronka_oneday['buyoutssumrub'] \
                    + voronka_oneday['cancelcount'] + voronka_oneday['cancelsumrub'] \
                    + voronka_oneday['avgpricerub'] + voronka_oneday['avgorderscountperday'] 

            voronka_oneday = voronka_oneday.query('sss > 0').drop(['end', 'sss'], axis=1).sort_values(by='date').reset_index(drop = True)

            #execute_query(connection, """DROP TABLE wb_voronka""")
            #create_sql_table('wb_voronka', voronka_oneday, '')
            #execute_query(connection, """ALTER TABLE wb_voronka
            #     ADD CONSTRAINT voronka_unique
            #        UNIQUE (nmid, date, partner)""")
            #execute_query(connection, """TRUNCATE TABLE wb_voronka""")

            execute_query(connection, f"""DELETE FROM wb_voronka WHERE date = '{current_date}'""", p=False)
            
            export_to_sql('wb_voronka', voronka_oneday)

        else:
            logg(f'Нет данных за дату {current_date}')

        #voronka = pd.concat([voronka, voronka_oneday])    
        current_date += delta

    #----------- while current_date               
    logg(f'------ Обновление воронки продаж завершено ------', 2)  
    return 


def etl_wb_all():
    """
    Обновление всех таблиц WB за раз, с "одной кнопки"
    Последовательно выполняются процедуры, описанные выше
    """  
    
    logg(f'------------- Начинаю обновление всех таблиц WB -------------- ', 2)
    
    try:
        etl_wb_orders()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении заказов: {e}')
    try:    
        etl_wb_sales()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении выкупов: {e}')
        
    try:
        etl_wb_income()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении поставок: {e}')

    try:
        etl_wb_stock()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении остатков на скаладах: {e}')
        
    try:
        etl_fbs_orders()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении сборочных заданий: {e}')
        
    try:
        etl_wb_cards()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении карточек товаров: {e}')
        
    try:
        etl_wb_goodsreturn()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновленииь возвратов: {e}')
    
    try:
        etl_wb_adv_expenses()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении затрат на рекламу: {e}')
        
    try:
        elt_wb_adv_stat()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении статистики рекламы: {e}')

    try:
        etl_wb_feedback()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении отзывов: {e}')

    #Убираю воронку, потому что надо обновлять данные за месяц, а то и больше, и это занимает порядка 10 минут
    #try:
    #    etl_wb_voronka()
    #except Exception as e:
    #    logg(f'!!!!!! Ошибка при обновлении воронки продаж: {e}')
            
    try:
        etl_wb_finreport()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении финансового отчета: {e}')        
    
    logg(f'------------ Обновление всех таблиц WB завершено ------------ ', 2)
    return


def etl_wb_everyday():
    """
    Обновление таблиц WB, в которых данные меняются часто:
    все таблицы, кроме карточек товаров, финотчета.
    Последовательно выполняются процедуры, описанные выше
    """  
    
    logg(f'------------- Начинаю ежедневное обновление таблиц WB -------------- ', 2)
    
    try:
        etl_wb_cards()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении карточек товаров: {e}')
            
    try:
        etl_wb_orders()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении заказов: {e}')
    try:    
        etl_wb_sales()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении выкупов: {e}')
        
    try:
        etl_wb_income()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении поставок: {e}')

    try:
        etl_wb_stock()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении остатков на скаладах: {e}')
        
    try:
        etl_fbs_orders()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении сборочных заданий: {e}')
      
    try:
        etl_wb_goodsreturn()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновленииь возвратов: {e}')
    
    try:
        etl_wb_adv_expenses()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении затрат на рекламу: {e}')
        
    try:
        elt_wb_adv_stat()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении статистики рекламы: {e}')

    try:
        etl_wb_feedback()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении отзывов: {e}')

    #Убираю воронку, потому что надо обновлять данные за месяц, а то и больше, и это занимает порядка 10 минут
    #try:
    #    etl_wb_voronka()
    #except Exception as e:
    #    logg(f'!!!!!! Ошибка при обновлении воронки продаж: {e}')


    logg(f'------------ Ежедневное Обновление таблиц WB завершено ------------ ', 2)
    return


def etl_wb_everyweek():
    """
    Обновление таблиц WB, в которых данные меняются не часто: карточки товаров, финотчет
    Последовательно выполняются процедуры, описанные выше
    """  
    
    logg(f'------------- Начинаю еженедельное обновление таблиц WB -------------- ', 2)

    try:
        etl_wb_finreport()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении финансового отчета: {e}')        
    
    logg(f'------------ Еженедельное обновление таблиц WB завершено ------------ ', 2)
    return



def start(event, context):
    """
    Корневая процедура скрипта
    Инициализация подключения к SQL
    Запуск по переданному параметру процедур опроса API WB
    """
    global log_lev
    log_lev = 0  
     
    try:
        command = event['messages'][0]['details']['payload']
    except:
        command = event
    
    print(f'***{datetime.now()} - Старт скрипта sher_wbapi_to_sql с параметром {command}***') 

    command = command + '   '
    cc = command.split(' ')
    func = cc[0]
    fromdate = cc[1]
    
    i = 0
    while i < 5:
        i +=1
        try:
            init_connection()
            base_reset()
            i = 10
        except Exception as e:
            print(f'Попытка {i} установить соединение с базой SQL провалилась {e}')
            time.sleep(3)

    #try:
    #    connection
        #if connection.closed == 0:

    try:
        connection
    except Exception as e:
        print(f'Нет соединения с SQL базой! {e}')
    else:    
        logg(f'*** Команда для sher_wbapi_to_sql - [{command}] ***', 1) 
        match func:
            case 'all': etl_wb_all()
            case 'everyday': etl_wb_everyday()
            case 'everyweek': etl_wb_everyweek()
            case 'orders': etl_wb_orders(fromdate)
            case 'sales': etl_wb_sales(fromdate)
            case 'income': etl_wb_income(fromdate)
            case 'stock':  etl_wb_stock(fromdate)
            case 'fbs_orders':  etl_fbs_orders()
            case 'goodsreturn':  etl_wb_goodsreturn(fromdate)
            case 'adv_expenses':  etl_wb_adv_expenses(fromdate)
            case 'adv_stat':  elt_wb_adv_stat(fromdate)
            case 'finreport':  etl_wb_finreport()
            case 'cards':  etl_wb_cards()
            case 'stocks_coords': wb_stocks_coords()
            case 'feedback': etl_wb_feedback()
            case 'voronka': etl_wb_voronka(fromdate)
            case _: 
                logg('Не задан параметр!') 
        logg(f'***** Выполнена команда - [{command}] *****', 1) 
        connection.close()

    print(f'***{datetime.now()} - Завершение работы скрипта sher_wbapi_to_sql***') 

    return