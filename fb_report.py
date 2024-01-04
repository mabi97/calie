import requests
from datetime import datetime, timedelta
import pyodbc 

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=14.225.9.147;'
                      'Database=Calie;'
                      'UID=Rossie;'
                      'PWD=Rossie2022@ReportMulti!@#$$;')
cursor = conn.cursor()

def exact(list): 
    result = []
    for i in list:
        if i is not None:
            result.append(i)
        else: pass
    if len(result) == 0:
        return 0
    else: return result[0]

def report_requests(account_id,token):
    host = "https://graph.facebook.com/v16.0/"
    parameter = "/insights?date_preset=today&fields=account_id%2Caccount_name%2Ccampaign_id%2Ccampaign_name%2Cadset_id%2Cadset_name%2Cad_id%2Cad_name%2Caccount_currency%2Cclicks%2Cimpressions%2Creach%2Cspend%2Ccreated_time%2Cactions%2Ccost_per_unique_click&filtering=%5B%7Bfield%3A%22action_type%22%2C%22operator%22%3A%22IN%22%2C%22value%22%3A%5B%22onsite_conversion.messaging_first_reply%22%2C%22comment%22%2C%22onsite_conversion.messaging_conversation_started_7d%22%2C%22post_engagement%22%2C%22link_click%22%5D%7D%5D&level=ad&time_increment=1&limit=200&transport=cors&access_token="
    #Khi muốn chọn ngày copy đoạn sau lên parameter: &time_range=%7Bsince%3A'2023-08-15'%2Cuntil%3A'2023-08-16'%7D
    url = host + account_id + parameter + token
    response = requests.request("GET", url)
    data = response.json()

    try:
        for item in data['data']:
            id = item['date_start'] + item['ad_id']
            if item.get('cost_per_unique_click'):
                if float(item['cost_per_unique_click']) != 0 or float(item['spend']):
                    clicks = round(float(item['spend'])/float(item['cost_per_unique_click']))
                else: clicks = 0
            else: clicks = 0
            row = id,\
                item['account_id'],\
                item['account_name'],\
                item['campaign_id'],\
                item['campaign_name'],\
                item['adset_id'],\
                item['adset_name'],\
                item['ad_id'],\
                item['ad_name'],\
                datetime.strptime(item['date_start'],'%Y-%m-%d'),\
                datetime.strptime(item['date_stop'],'%Y-%m-%d'),\
                item['account_currency'],\
                float(item['spend']),\
                int(item['impressions']) if item.get('impressions') else None,\
                int(item['reach']) if item.get('reach') else None,\
                clicks,\
                int(exact([x['value'] if x['action_type']=='onsite_conversion.messaging_first_reply' else None for x in item['actions']])) if item.get('actions') else 0,\
                int(exact([x['value'] if x['action_type']=='comment' else None for x in item['actions']])) if item.get('actions') else 0,\
                int(exact([x['value'] if x['action_type']=='onsite_conversion.messaging_conversation_started_7d' else None for x in item['actions']])) if item.get('actions') else 0,\
                int(exact([x['value'] if x['action_type']=='post_engagement' else None for x in item['actions']])) if item.get('actions') else 0,\
                int(exact([x['value'] if x['action_type']=='link_click' else None for x in item['actions']])) if item.get('actions') else 0,\
                datetime.strptime(item['created_time'],'%Y-%m-%d')
            query = f"INSERT INTO FB_AdReport VALUES ({','.join(['?' for val in row])})"
            cursor.execute("DELETE FB_AdReport WHERE id = '" + f'{id}' + "'")
            cursor.execute(query, row)
            #with open('tracking.txt', 'a') as file:
            #file.write('\n' + str(account_id) + ' ' + str(len(data['data'])) + ' ' +str(datetime.now()))
    except:
        try:
            with open('tracking.txt', 'a') as file:
                file.write('\n' + 'AdReport!!! ' + data['error']['message'] + '!!! ' + str(datetime.now()))
        except:
            with open('tracking.txt', 'a') as file:
                file.write('\n' + 'Lỗi khác')        

def ad_request(ad_id,token):
    host = "https://graph.facebook.com/v16.0/"
    parameter = "/adcreatives?fields=actor_id%2Ceffective_object_story_id&transport=cors&access_token="
    url = host + ad_id + parameter + token
    response = requests.request("GET", url)
    data = response.json()
    
    try:
        for item in data['data']:
            id = ad_id 
            row = id,\
                item['effective_object_story_id'] if item.get('effective_object_story_id') else None,\
                item['actor_id'] if item.get('actor_id') else None
            
            query = f"INSERT INTO FB_Ad VALUES ({','.join(['?' for val in row])})"
            cursor.execute("DELETE FB_Ad WHERE id = '" + f'{id}' + "'")
            cursor.execute(query, row)
    except:
        try:
            with open('tracking.txt', 'a') as file:
                file.write('\n' + 'AdAccount!!! ' + data['error']['message'] + '!!! ' + str(datetime.now()))
        except: 
            with open('tracking.txt', 'a') as file:
                file.write('\n' + 'Lỗi code ' + str(datetime.now()))                   

cursor.execute("SELECT id, token FROM FB_AdAccount")
for i in cursor.fetchall():
    report_requests(i[0],i[1])


conn.commit()

cursor.execute("""SELECT ad_id, token
FROM 
(SELECT DISTINCT ad_id, token
FROM Calie.dbo.FB_AdReport
LEFT JOIN 
    (SELECT REPLACE(id,'act_','') id, token
    FROM Calie.dbo.FB_AdAccount) TB11
ON FB_AdReport.account_id = TB11.id) TB1
LEFT JOIN Calie.dbo.FB_Ad
ON TB1.ad_id = FB_Ad.id
WHERE FB_Ad.id is NULL
""")

for i in cursor.fetchall():
    ad_request(i[0],i[1])


conn.commit()
conn.close()  