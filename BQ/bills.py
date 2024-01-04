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
table_id = 'bills'

# Tạo đối tượng BigQuery table
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)
current_date = datetime.now().date()
today_date = current_date.strftime("%Y-%m-%d")

def main_request(page):

    url = "https://open.nhanh.vn/api/bill/search"

    payload = {'version': '2.0',
    'appId': '73670',
    'businessId': '41931',
    'accessToken': 'TgtXPMjeE6BtzFwgEMFJmGFRvwTDHQVgUnRIy4UvbX4YJME8QLh4LA5gz6uiS9eTtqYmQR0JHVsJDeclAffAR21ieqru6jFU9lG4BMTbtL5rlTw2rIVZHFm4aCaQdPpkIcTIce1ucMR90myfCb5NYJZmIvVP8vgwIbDspERlg1o2v6Jne04FCD6BTt5QWu6g5tdL',
    'data': '{"fromDate":"' + today_date + '","toDate":"' + today_date + '" ,"page":' + str(page) + '}' #"fromDate":"' + today_date + '","toDate":"' + today_date + '"    "fromDate":"2023-10-17","toDate":"2023-10-17"
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
    rows_to_insert = []

    for key1, order in orders.items():
        try:
            if order["mode"] == "5" or order["mode"] == "2":
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
                        product["price"],
                        product["discount"],
                        product["money"],
                        order["type"],
                        order["mode"],
                        order["saleName"],
                    ]

                    char = [None if (val is None or val == '') else val for val in char]
                    rows_to_insert.append(tuple(char))


        except Exception as e:
            print(f"Error processing order {key1}: {str(e)}")
    
    if rows_to_insert:
        errors = client.insert_rows(table, rows_to_insert)
        if errors:
            print(f"Errors while inserting data: {errors}")

delete_job = client.query("DELETE FROM `mindful-rhythm-344416.Calie_Nhanh.bills` WHERE date(create_time) = '" + today_date + "'")
delete_job.result()

try:
    for i in range(1, get_page(main_request(1)) + 1):
        get_orders(main_request(i))
except Exception as e:
    print(f"Error during data extraction: {str(e)}")
