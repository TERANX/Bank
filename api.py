from fastapi import FastAPI
app = FastAPI()
import pymysql
import datetime

def get_valute_rate_from_db(connection, cursor, valute):
    today = datetime.datetime.now().strftime('%Y%m%d')
    select_str = f'SELECT rate  from valute_rate  WHERE valute = "{valute}" AND  date  = "{today}";'
    cursor.execute(select_str)
    rate = float(cursor.fetchall()[0][0])
    return rate
def connect_to_db():
    connection = pymysql.connect(host='192.168.0.108', port=3306, user='obmen', password='123456', db='bank')
    cursor = connection.cursor()
    return connection, cursor


@app.get("/")
def root():
    return 'привет'
@app.get("/users")
def users():
    return ('kirill', 'ivan','pavel','sergey')
@app.get("/valutes/{valute_name}")
def get_valute_rate(valute_name):
    connection, cursor = connect_to_db()
    print(valute_name, connection, cursor)
    rate = get_valute_rate_from_db(connection,cursor,valute_name)
    return {valute_name:rate}

@app.get("/convert")
def convert_valute(fv,sv,count):
    connection, cursor = connect_to_db()
    fv_rate = get_valute_rate_from_db(connection, cursor, fv)
    sv_rate = get_valute_rate_from_db(connection,cursor,sv)
    OUTVALUTE_COUNT = round((float(count) * fv_rate) / sv_rate, 2)

    return OUTVALUTE_COUNT
print('+1')

