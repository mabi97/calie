#cập nhật đơn hàng online theo update time
import requests
import json
from datetime import datetime, timedelta
import pyodbc


#kết nối tới databse
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=14.225.9.147;'
                      'Database=Calie;'
                      'UID=Rossie;'
                      'PWD=Rossie2022@ReportMulti!@#$$;')
cursor = conn.cursor()

def main_request(page): #lấy dữ liệu từng page trả về file json
    current_time = datetime.now()
    before_70_minutes = current_time - timedelta(minutes=70)
    to_time = before_70_minutes.strftime("%Y-%m-%d %H:%M:%S")

    url = "https://open.nhanh.vn/api/order/index"

    payload = {'version': '2.0',
    'appId': '73670',
    'businessId': '41931',
    'accessToken': 'TgtXPMjeE6BtzFwgEMFJmGFRvwTDHQVgUnRIy4UvbX4YJME8QLh4LA5gz6uiS9eTtqYmQR0JHVsJDeclAffAR21ieqru6jFU9lG4BMTbtL5rlTw2rIVZHFm4aCaQdPpkIcTIce1ucMR90myfCb5NYJZmIvVP8vgwIbDspERlg1o2v6Jne04FCD6BTt5QWu6g5tdL',
    'data': '{"updatedFromDateTime":' + '"' + to_time + '"' + ',"page":' + str(page) + '}'
    }
    files=[

    ]
    headers = {}

    response = requests.request("POST", url, headers=headers, data=payload, files=files)

    return response.json()

def get_page(response): #lấy số page
    return response['data']['totalPages']

def get_orders(response): #insert hoặc update từng đơn hàng vào databse
    orders = response["data"]["orders"]

    for key, order in orders.items():
        products = order["products"]
        for product in products:
            id = str(order["id"]) + product["productId"]
            char = [
                id,
                order["id"],
                order["createdDateTime"],
                order["depotId"],
                order["customerId"],
                product["productCode"],
                product["quantity"],
                product["price"],
                product["discount"],
                order["saleChannel"],
                order["statusCode"],
                order["trafficSourceName"],
                order["typeId"],
                order["carrierName"],
                order["customerCity"],
                order["shipFee"],
                order["customerShipFee"],
                order["codFee"],
                order["returnFee"],
                order["calcTotalMoney"],
                len(products),
                order["moneyTransfer"], 
                #order["deliveryDate"] #order.get("deliveryDate"),
                #order.get("sendCarrierDate")
            ]
            
            char = [None if (val is None or val == '') else val for val in char]
            query_1 = f"INSERT INTO Orders VALUES ({','.join(['?' for val in char])})"
            cursor.execute("DELETE Orders WHERE id = '" + f'{id}' + "'")
            cursor.execute(query_1, char)

for i in range(1,get_page(main_request(1))+1):
    get_orders(main_request(i))


conn.commit()
conn.close()