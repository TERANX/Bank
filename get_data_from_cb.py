
import configparser
import requests
import pymysql
import datetime
import sys
import os
import logging


def notification():
    pass


def get_data_from_cb(url):
    logging.debug('Start func get_data_from_cb')
    result = requests.get(url)
    logging.debug('data successfully received from the Central Bank')
    raw_json = result.json()['Valute']
    logging.debug('json has been successfully received')
    real_rates = {}
    logging.debug('An empty dictionary has been created')

    for valute in raw_json:
        raw_rate = float(raw_json[valute]['Value'])
        nominal = int(raw_json[valute]['Nominal'])
        real_rate = round(raw_rate / nominal, 3)
        real_rates[valute] = real_rate
    logging.debug(f'fuction return data {real_rates}')
    return real_rates


def insert_data_to_db(connection, cursor, valute_dict):
    today = datetime.datetime.now().strftime('%Y%m%d')
    for valute in valute_dict.keys():
        rate = valute_dict[valute]
        insert_str = f'INSERT into valute_rate values("{today}", "{valute}","{rate}")'
        cursor.execute(insert_str)

    connection.commit()

    return True


def get_data_from_config():
    config = configparser.ConfigParser()
    config.read('get_data_from_cb.conf')
    cburl = config.get('general', 'cburl')
    host = config.get('database', 'host')
    port = int(config.get('database', 'port'))
    user = config.get('database', 'user')
    password = config.get('database', 'pass')
    db = config.get('database', 'db')
    return cburl, host, port, user, password, db


def connect_to_db(host, port, user, password, db):
    connection = pymysql.connect(host=host, port=port, user=user, password=password, db=db)
    cursor = connection.cursor()
    return connection, cursor


if __name__ == '__main__':
    logging.basicConfig(filename='get_data_from_cb.log', level=logging.DEBUG,  format='[%(asctime)s] [%(levelname)s] => %(message)s')
    logging.info('Попытка получить дату')
    today = datetime.datetime.now().strftime('%Y%m%d')
    logging.info(f'Дата успешно получена {today}')

    filename = f'{today}.ok'
    logging.info('Начинаю проверку флагового файла')
    if os.path.exists(filename):
        logging.info('Заканчиваю выполнение - флаговый файл сушествует')
        sys.exit()
    logging.info('Продолжаю выполнение - флаговый файл отсутствует')

    insert_result = False
    logging.info('Запускаю получение данных из конфига')
    cburl, host, port, user, password, db = get_data_from_config()
    logging.info('Данные из конфига получены')
    try:
        logging.info('Пытаюсь получить данные из ЦБ')
        valute_dict = get_data_from_cb(cburl)
        logging.info('Получил данные из ЦБ')
        logging.info('Пытаюсь создать подключение к БД')
        connection, cursor = connect_to_db(host, port, user, password, db)
        logging.info('Создал подключение к БД')
        logging.info('Пытаюсь внести данные в БД')
        insert_result = insert_data_to_db(connection, cursor, valute_dict)
        logging.info('Внес данные в БД')
        logging.info('Проверяю результат внесения в бд')
        if insert_result:
            open(filename, 'a').close()
            logging.info('Данные внесены в БД, флаговый файл создан!')

    except requests.exceptions.ConnectionError as CE:
        logging.error(f'Не удалось подключиться к сайту ЦБ \n{CE}')
    except requests.exceptions.JSONDecodeError as JDE:
        logging.error(f'Не удалось разобрать json \n{JDE}')
