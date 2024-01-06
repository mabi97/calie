#lấy dữ liệu account chạy ad trên fb và tiktok
import requests
from datetime import datetime, timedelta
import pyodbc

fb_token_list = ["EAAI9vVVEJgwBO2JHY0NWNBZAajoFdLUitvQt4MgntZCE2ZAMCgV63JhYN69roNOO6TwXR4igKuHgTfq9HhbI6fQlY1JFdQUW1itVxqU1fM1J6JTpMixJGHhTXf6vzUYkXQO6jpcdX3juLVsZCC7VgZByc5nwr703DNZAvQZCRRYoRz8vcNvZAweF7oghoGzNCD3d7RgD85vs"]
#mã token của facebook sẽ hết hạn sau 2 tháng

tiktok_token_list = ["bcee4384ce761bba2efe90db2fd04e109f05f933"]
#không bị hết hạn

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=14.225.9.147;'
                      'Database=Calie;'
                      'UID=Rossie;'
                      'PWD=Rossie2022@ReportMulti!@#$$;')
cursor = conn.cursor()

def fb_adaccount_requests(token):
    host = "https://graph.facebook.com/v16.0/me?fields=adaccounts%7Bbalance%2Camount_spent%2Ccurrency%2Cspend_cap%2Cfunding_source_details%2Caccount_status%2Cname%7D%2Cid%2Cname&limit=100&access_token="
    url = host + token
    response = requests.request("GET", url)
    data = response.json()

    try:
        for item in data['adaccounts']['data']:
            id = item['id']
            try: 
                bannk_account = item['funding_source_details']['display_string']
            except:
                bannk_account = None
            row = item['id'],\
                item['name'],\
                token
            query = f"INSERT INTO FB_AdAccount VALUES ({','.join(['?' for val in row])})"
            cursor.execute("DELETE FB_AdAccount WHERE id = '" + f'{id}' + "'")
            cursor.execute(query, row)
        #with open('tracking.txt', 'a') as file:
            #file.write('\n' + 'AdAccount!!! Success!!! ' + str(datetime.now()))
    except: 
        try:
            with open('tracking.txt', 'a') as file:
                file.write('\n' + 'AdAccount!!! ' + data['error']['message'] + '!!! ' + str(datetime.now()))
        except: 
            with open('tracking.txt', 'a') as file:
                file.write('\n' + 'Lỗi code ' + str(datetime.now()))

def tiktok_adaccount_requests(token):

  url = "https://business-api.tiktok.com/open_api/v1.3/oauth2/advertiser/get/?app_id=7215062312636383234&secret=48628c61709f4ac9bd6b583b35f061370508c8fa"

  headers = {
    'Access-Token': token
  }

  response = requests.request("GET", url, headers=headers)
  data = response.json()

  for i in data['data']['list']:
      id = i['advertiser_id']
      row = id, i['advertiser_name'], token

      query = f"INSERT INTO Tiktok_Advertiser VALUES ({','.join(['?' for val in row])})"
      cursor.execute("DELETE Tiktok_Advertiser WHERE id = '" + f'{id}' + "'")
      cursor.execute(query, row)

for i in fb_token_list:
    fb_adaccount_requests(i)

for i in tiktok_token_list:
    tiktok_adaccount_requests(i)

conn.commit()
conn.close()