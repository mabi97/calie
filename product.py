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
    before = current_time - timedelta(hours=24)
    time = before.strftime("%Y-%m-%d %H:%M:%S")

    url = "https://open.nhanh.vn/api/product/search"

    payload = {'version': '2.0',
    'appId': '73670',
    'businessId': '41931',
    'accessToken': 'TgtXPMjeE6BtzFwgEMFJmGFRvwTDHQVgUnRIy4UvbX4YJME8QLh4LA5gz6uiS9eTtqYmQR0JHVsJDeclAffAR21ieqru6jFU9lG4BMTbtL5rlTw2rIVZHFm4aCaQdPpkIcTIce1ucMR90myfCb5NYJZmIvVP8vgwIbDspERlg1o2v6Jne04FCD6BTt5QWu6g5tdL',
    'data': '{"updatedFromDateTime":' + '"' + time + '"' + ',"page":' + str(page) + '}'
    }
    files=[

    ]
    headers = {}

    response = requests.request("POST", url, headers=headers, data=payload, files=files)

    return response.json()

def get_page(response):
    return response['data']['totalPages']

def get_products(response):
    products = response["data"]["products"]

    for key1, product in products.items():
        id = product['idNhanh']
        char = [
            product['idNhanh'],
            product['parentId'],
            product['brandId'],
            product['brandName'],
            product['status'],
            product['typeId'],
            product['typeName'],
            product['categoryId'],
            product['code'],
            product['barcode'],
            product['name'],
            product['otherName'],
            product['oldPrice'],
            product['importPrice'],
            product['price'],
            product['image'],
            product['previewLink'],
            product['shippingWeight'],
            product['createdDateTime'],
            product['inventory']['remain'],
            product['inventory']['available'],
            product['avgCost'],
            product['internalCategoryId'],
        ]

        char = [None if (val is None or val == '') else val for val in char]
        query_1 = f"INSERT INTO Products VALUES ({','.join(['?' for val in char])})"
        cursor.execute("DELETE Products WHERE idNhanh = '" + f'{id}' + "'")
        cursor.execute(query_1, char)

for i in range(1,get_page(main_request(1))+1):
    get_products(main_request(i))

conn.commit()
conn.close()
