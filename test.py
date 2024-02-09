import pymysql

connection = pymysql.connect(host='192.168.0.108', user='obmen',
                             password='123456', database='bank',
                             port = 3306)
cursor = connection.cursor()
cursor.execute('select * from valute_rate;')

data = cursor.fetchall()

print(data)
