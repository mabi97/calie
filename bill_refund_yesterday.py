import requests
import json
from datetime import datetime, timedelta
import pyodbc

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=14.225.9.147;'
                      'Database=Calie;'
                      'UID=Rossie;'
                      'PWD=Rossie2022@ReportMulti!@#$$;')
cursor = conn.cursor()

def main_request(page):
    current_time = datetime.now()
    yesterday = current_time - timedelta(days=1)

    yesterday_time = yesterday.strftime("%Y-%m-%d")

    url = "https://open.nhanh.vn/api/bill/search"

    payload = {'version': '2.0',
    'appId': '73670',
    'businessId': '41931',
    'accessToken': 'TgtXPMjeE6BtzFwgEMFJmGFRvwTDHQVgUnRIy4UvbX4YJME8QLh4LA5gz6uiS9eTtqYmQR0JHVsJDeclAffAR21ieqru6jFU9lG4BMTbtL5rlTw2rIVZHFm4aCaQdPpkIcTIce1ucMR90myfCb5NYJZmIvVP8vgwIbDspERlg1o2v6Jne04FCD6BTt5QWu6g5tdL',
    'data': '{"fromDate":"' + yesterday_time + '","toDate":"' + yesterday_time + '","type":1,"mode":2,"page":' + str(page) + '}'
    }
    files=[

    ]
    headers = {}

    response = requests.request("POST", url, headers=headers, data=payload, files=files)

    return response.json()

def get_page(response):
    return response['data']['totalPages']

def get_orders(response):
    orders = response["data"]["bill"]

    for key1, order in orders.items():
        products = order["products"]
        for key2, product in products.items():
            id = str(order["id"]) + product["id"]
            char = [
                id,
                order["id"],
                order["createdDateTime"],
                order["depotId"],
                order["customerId"],
                product["code"],
                product["quantity"],
                0 - int(product["price"]),
                product["discount"],
                0 - int(product["money"])
            ]
            
            char = [None if (val is None or val == '') else val for val in char]
            query_1 = f"INSERT INTO Bills VALUES ({','.join(['?' for val in char])})"
            cursor.execute("DELETE Bills WHERE id = '" + f'{id}' + "'")
            cursor.execute(query_1, char)

try:
    for i in range(1,get_page(main_request(1))+1):
        get_orders(main_request(i))
except: pass


conn.commit()
conn.close()


