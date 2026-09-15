import json

import requests

url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/SBER/trades.json"
resp = requests.get(url)
data = resp.json()

"""
print()
print("data:\n", data)
print()
print("data['trades'].keys():\n", data["trades"].keys())
print()
print("data['trades']['data']:\n", data["trades"]["data"])
print()
"""

# Шаг 1: какие вообще есть верхнеуровневые блоки в ответе?
print()
print("Ключи верхнего уровня:")
for col in list(data.keys()):
    print("-", col)
print()
# Шаг 2: реальные названия полей (колонок) сделок
print("Колонки 'trades':")
for col in data["trades"]["columns"]:
    print("-", col)
print()
print("Колонки 'dataversion':")
for col in data["dataversion"]["columns"]:
    print("-", col)
print()
print("Колонки 'trades_yields':")
for col in data["trades_yields"]["columns"]:
    print("-", col)
print()
# Шаг 3: первые несколько строк — смотрим глазами, что реально прилетает
for row in data["trades"]["data"][:5]:
    print(row)

print("Всего строк в ответе:", len(data["trades"]["data"]))

print()

url_page2 = url + "?start=100"
resp2 = requests.get(url_page2)
data2 = resp2.json()
cnt_rows = len(data2["trades"]["data"])
print("Строк на второй странице:", cnt_rows)
if cnt_rows:
    print("Первая строка второй страницы:", data2["trades"]["data"][0])
    print("Последняя строка первой страницы:", data["trades"]["data"][-1])

print()
"""
url_3 = "https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/SBER.json?from=2026-09-07&till=2026-09-13"
resp_3 = requests.get(url_3)
data_3 = resp_3.json()


print("Ключи верхнего уровня:", list(data_3.keys()))
print("Колонки history:", data_3["history"]["columns"])

for row in data_3["history"]["data"][:5]:
    print(row)

print("Всего строк в ответе:", len(data_3["history"]["data"]))
"""
