import requests
from datetime import datetime, timedelta
import pyodbc

def tiktok_request(advertiser_id,token,advertiser_name):
  start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d') 
  end_date = datetime.now().strftime('%Y-%m-%d')
  url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/?advertiser_id=" + f'{advertiser_id}' + "&page_size=200&report_type=BASIC&dimensions=[\"ad_id\", \"stat_time_day\"]&data_level=AUCTION_AD&start_date="+ f'{start_date}' +"&end_date="+ f'{end_date}' +"&metrics=[\"ad_name\",\"adgroup_name\",\"campaign_name\",\"objective_type\",\"currency\",\"spend\",\"impressions\",\"reach\",\"clicks\",\"conversion\"]"
  payload={}
  headers = {
    'Access-Token': token,
    'Content-Type': 'application/json'
  }
  response = requests.request("GET", url, headers=headers, data=payload)
  data = response.json()

  try: 
    for i in data['data']['list']:
        date = datetime.strptime(i['dimensions']['stat_time_day'], '%Y-%m-%d %H:%M:%S')
        ad_id = i['dimensions']['ad_id']
        row = date,\
            ad_id,\
            i['metrics']['ad_name'],\
            i['metrics']['adgroup_name'],\
            i['metrics']['campaign_name'],\
            i['metrics']['currency'],\
            float(i['metrics']['spend']),\
            int(i['metrics']['impressions']),\
            int(i['metrics']['reach']),\
            int(i['metrics']['clicks']),\
            int(i['metrics']['conversion']),\
            advertiser_name,\
            i['metrics']['objective_type']

        query = f"INSERT INTO Tiktok_AdReport VALUES ({','.join(['?' for val in row])})"
        cursor.execute("DELETE Tiktok_AdReport WHERE date = '" + f'{date}' + "' AND ad_id = '" + f'{ad_id}' + "'")
        cursor.execute(query, row)  
  except: pass

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=14.225.9.147;'
                      'Database=Calie;'
                      'UID=Rossie;'
                      'PWD=Rossie2022@ReportMulti!@#$$;')
cursor = conn.cursor()

cursor.execute("""SELECT id, token, name
                  FROM Calie.dbo.Tiktok_Advertiser""")

for i in cursor.fetchall():
    tiktok_request(i[0],i[1],i[2])

conn.commit()
conn.close()