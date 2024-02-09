import requests

INVALUTE = input('Введите валюту, которую вы хотите поменять: ')
OUTVALUTE = input('Введите валюту, которую вы хотите приобрести: ')
INVALUTE_COUNT = float(input('Сколько вы хотите поменять: '))

get_str = f'http://192.168.0.106:8000/convert?fv={INVALUTE}&sv={OUTVALUTE}&count={INVALUTE_COUNT}'
print(get_str)
OUTVALUTE_COUNT = requests.get(get_str).json()
print(OUTVALUTE_COUNT)
print(f'Выдать на руки {OUTVALUTE_COUNT} {OUTVALUTE}')