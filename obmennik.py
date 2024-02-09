import requests
import pymysql
import sys
import os
import logging
import configparser
import datetime
import redis



def get_data_from_config():
    config = configparser.ConfigParser()
    logging.debug('Пытаюсь открыть файл конфигурации')
    config.read('obmennik.conf')
    logging.debug('Открыл файл конфигурации')
    logging.debug('Пытаюсь прочесть данные из файла конфигурации')
    db_host = config.get('database', 'host')
    db_port = int(config.get('database', 'port'))
    db_user = config.get('database', 'user')
    db_password = config.get('database', 'pass')
    db = config.get('database', 'db')
    redis_host = config.get('redis', 'host')
    redis_port = int(config.get('redis', 'port'))
    redis_pass = config.get('redis', 'pass')
    logging.debug('Прочел данные из файла конфигурации')
    return db_host, db_port, db_user, db_password, db, redis_host, redis_port, redis_pass


def connect_to_db(host, port, user, password, db):
    logging.debug('Пытаюсь создать подключение к БД')
    connection = pymysql.connect(host=host, port=port, user=user, password=password, db=db)
    logging.debug('Создал подключение к БД')
    cursor = connection.cursor()
    return connection, cursor


def connect_to_redis(redis_host, redis_port, redis_pass):
    logging.debug('Пытаюсь создать подключение к redis')
    redis_conn = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_pass)
    logging.debug('Создал подключение к redis')
    return redis_conn


def get_rate_from_redis(redis_conn, valute):
    logging.debug('Пытаюсь прочесть курс валюты из redis')
    rate = redis_conn.get(valute)
    if rate:
        rate = float(rate.decode())
        logging.debug('Прочел курс валюты из redis')
    return rate


def set_rate_to_redis(redis_conn, valute, valute_rate):
    logging.debug('Пытаюсь записать курс валюты в redis')
    redis_conn.set(valute, valute_rate, ex=30)
    logging.debug('Записал курс валюты в redis')


def set_rate_from_redis():
    return True


def get_valute_rate(connection, cursor, valute):
    logging.debug('Пытаюсь получить сегодняшнюю дату')
    today = datetime.datetime.now().strftime('%Y%m%d')
    logging.debug('Получил сегодняшнюю дату')
    logging.debug('Пытаюсь получить последнюю дату в БД')
    seelect_date_str = 'SELECT date FROM valute_rate ORDER BY date DESC LIMIT 1;'
    cursor.execute(seelect_date_str)
    last_date_in_base = cursor.fetchall()[0][0]
    logging.debug('Получил последнюю дату в БД')
    print('last_date_in_base ', last_date_in_base)
    today = today if today == last_date_in_base else last_date_in_base
    logging.debug('Скорректировал дату для запроса')
    logging.debug('Пытаюсь создать запрос к БД на получение курса валюты')
    select_str = f'SELECT rate  from valute_rate  WHERE valute = "{valute}" AND  date  = "{today}";'
    cursor.execute(select_str)
    logging.debug('Создал запрос к БД на получение курса валюты')
    logging.debug('Пытаюсь получить курс валюты из результатов запроса')
    rate = float(cursor.fetchall()[0][0])
    logging.debug('Получил курс валюты из результатов запроса')
    print('rate ', rate)
    return rate


if __name__ == '__main__':
    logging.basicConfig(filename='obmennik.log', level=logging.DEBUG,
                        format='[%(asctime)s] [%(levelname)s] => %(message)s')
    connection = None
    INVALUTE = input('Введите валюту, которую вы хотите поменять: ')
    OUTVALUTE = input('Введите валюту, которую вы хотите приобрести: ')
    INVALUTE_COUNT = float(input('Сколько вы хотите поменять: '))
    start_time = datetime.datetime.now()
    try:
        logging.info('Запускаю получение данных из конфига')
        db_host, db_port, db_user, db_password, db, redis_host, redis_port, redis_pass = get_data_from_config()
        logging.info('Данные из конфига получены')
        logging.info('Пытаюсь подключится к redis')
        red_conn = connect_to_redis(redis_host, redis_port, redis_pass)
        logging.info('Соединение с redis установлено')
        INVALUTE_RATE = get_rate_from_redis(red_conn, INVALUTE)
        OUTVALUTE_RATE = get_rate_from_redis(red_conn, OUTVALUTE)
        logging.info(f'{INVALUTE_RATE}, {OUTVALUTE_RATE}, из redis')
        print(INVALUTE_RATE, OUTVALUTE_RATE, 'из redis')
        if not INVALUTE_RATE:
            logging.info('Пытаюсь подключится к базе')
            connection, cursor = connect_to_db(db_host, db_port, db_user, db_password, db)
            logging.info('Создал подключение к БД')
            logging.info('Пытаюсь получить курс первой валюты из базы')
            INVALUTE_RATE = get_valute_rate(connection, cursor, INVALUTE)
            logging.info('Получил курс первой валюты из базы')
            logging.info('Пытаюсь записать первую валюту в redis')
            set_rate_to_redis(red_conn, INVALUTE, INVALUTE_RATE)
            logging.info('Записал первую валюту в redis')
            print(INVALUTE, INVALUTE_RATE, 'INVALUTE_RATE из DB')
        if not OUTVALUTE_RATE:
            if not connection:
                # print (connection)
                logging.info('Пытаюсь подключится к базе')
                connection, cursor = connect_to_db(db_host, db_port, db_user, db_password, db)
                logging.info('Создал подключение к БД')
            logging.info('Пытаюсь получить курс второй валюты из базы')
            OUTVALUTE_RATE = get_valute_rate(connection, cursor, OUTVALUTE)
            logging.info('Получил курс второй валюты из базы')
            print(OUTVALUTE, OUTVALUTE_RATE, 'OUTVALUTE_RATE из DB')
            logging.info('Пытаюсь записать вторую валюту в redis')
            set_rate_to_redis(red_conn, OUTVALUTE, OUTVALUTE_RATE)
            logging.info('Записал вторую валюту в redis')
        logging.info('Пытаюсь посчитать количество валюты на руки')
        OUTVALUTE_COUNT = round((INVALUTE_COUNT * INVALUTE_RATE) / OUTVALUTE_RATE, 2)
        logging.info('Посчитал количество валюты на руки')
        print(f'Выдать на руки {OUTVALUTE_COUNT} {OUTVALUTE}')
        end_time = datetime.datetime.now()
        work_time = end_time - start_time
        print(f'Время работы {work_time}')

    except configparser.NoSectionError as NSE:
        logging.error(f'Не удалось прочитать секцию в конфиге \n{NSE}')
    except redis.exceptions.AuthenticationError as RAE:
        logging.error(f'Не удалось подключится к redis \n{RAE}')
    except pymysql.err.OperationalError as POE:
        logging.error(f'Не удалось подключится к БД \n{POE}')
    except AttributeError as AE:
        logging.error(f'Не удалось получить курс из БД \n{AE}')
