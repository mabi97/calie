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

def main_request():
    current_date = datetime.now().date()
    today_date = current_date.strftime("%Y-%m-%d")

    url = "https://open.nhanh.vn//api/product/category"

    payload = {'version': '2.0',
    'appId': '73670',
    'businessId': '41931',
    'accessToken': 'TgtXPMjeE6BtzFwgEMFJmGFRvwTDHQVgUnRIy4UvbX4YJME8QLh4LA5gz6uiS9eTtqYmQR0JHVsJDeclAffAR21ieqru6jFU9lG4BMTbtL5rlTw2rIVZHFm4aCaQdPpkIcTIce1ucMR90myfCb5NYJZmIvVP8vgwIbDspERlg1o2v6Jne04FCD6BTt5QWu6g5tdL'
    }
    files=[

    ]
    headers = {}

    response = requests.request("POST", url, headers=headers, data=payload, files=files)

    return response.json()

def get_caregory(response):

    parents = response["data"]

    for parent in parents:
        if parent.get("childs"):
            childs = parent["childs"]
            for child in childs:
                id = child["id"]
                char = [
                    id,
                    child["name"],
                    child["code"],
                    child["status"],
                    child["image"],
                    child["order"],
                    parent["id"],
                    parent["name"],
                    parent["code"],
                    parent["order"]
                ]
                char = [None if (val is None or val == '') else val for val in char]
                query_1 = f"INSERT INTO Product_Caregory VALUES ({','.join(['?' for val in char])})"
                cursor.execute("DELETE Product_Caregory WHERE id = '" + f'{id}' + "'")
                cursor.execute(query_1, char)
        else:
                id = parent["id"]
                char = [
                    id,
                    parent["name"],
                    parent["code"],
                    parent["status"],
                    parent["image"],
                    parent["order"],
                    parent["id"],
                    parent["name"],
                    parent["code"],
                    parent["order"]
                ]
                
                char = [None if (val is None or val == '') else val for val in char]
                query_1 = f"INSERT INTO Product_Caregory VALUES ({','.join(['?' for val in char])})"
                cursor.execute("DELETE Product_Caregory WHERE id = '" + f'{id}' + "'")
                cursor.execute(query_1, char)

get_caregory(main_request())

conn.commit()
conn.close()