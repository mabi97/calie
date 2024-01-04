import os
from google.cloud import bigquery
from datetime import datetime
import requests
import json
from datetime import datetime, timedelta

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'mindful-rhythm-344416-fd1a2d1aad0f.json'

client = bigquery.Client()

project_id = 'mindful-rhythm-344416'
dataset_id = 'Calie_Nhanh'
table_id = 'products'

table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)

def main_request(page):
    current_time = datetime.now()
    before = current_time - timedelta(hours=30)
    time = before.strftime("%Y-%m-%d %H:%M:%S") #'{"updatedFromDateTime":' + '"' + time + '"' + ',"page":' + str(page) + '}'

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
    rows_to_insert = []

    for key1, product in products.items():
        id = product['idNhanh']
        remains = product['inventory']['depots']
        if remains == []:
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
                    None,
                    None,
                    product['avgCost'],
                    product['internalCategoryId'],
                ]
            char = [None if (val is None or val == '') else val for val in char]
            rows_to_insert.append(tuple(char))


        else:
            for key2, remain in remains.items():
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
                    key2,
                    remain['remain'],
                    product['avgCost'],
                    product['internalCategoryId'],
                ]

                char = [None if (val is None or val == '') else val for val in char]
                rows_to_insert.append(tuple(char))
                    
        delete_job = client.query("DELETE FROM `mindful-rhythm-344416.Calie_Nhanh.products` WHERE idNhanh = '" + id + "'")
        delete_job.result()     
    
    if rows_to_insert:
        errors = client.insert_rows(table, rows_to_insert)
        if errors:
            print(f"Errors while inserting data: {errors}")

for i in range(1, get_page(main_request(1)) + 1):
    get_products(main_request(i))