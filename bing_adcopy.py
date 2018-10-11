import datetime
import psycopg2 as pc
import db_credentials as db


def delete_ads():
    deletion_query = 'DELETE FROM heycar.mkt_ad  WHERE mkt_source=\'bing_ads\';'
    con = pc.connect(dbname = db.credentials['db_name'] , host = db.credentials['db_host'] , port = db.credentials['db_port'], user = db.credentials['db_user'], password = db.credentials['db_pw'])
    cur = con.cursor()
    cur.execute(deletion_query)
    con.commit()


def insert_ad_details(adgroup_ids, campaignmanagement_service):
    insertion_query = 'INSERT INTO heycar.mkt_ad (dwh_date, mkt_source, ad_id, name, status, final_url, type, adgroup_id) VALUES '
    dwh_date = datetime.datetime.today().strftime('%Y-%m-%d')
    dwh_date = "'" + str(dwh_date) + "'"

    ad_types = {'AdType': ['AppInstall',
    'DynamicSearch',
    'ExpandedText',
    'Product',
    'ResponsiveAd',
    'Text']}

    for adgroup_id in adgroup_ids:
        try: #adgroup_id might be from a different account
            response = campaignmanagement_service.GetAdsByAdGroupId(
                AdGroupId = adgroup_id,
                AdTypes = ad_types)
            for ad in response[0]:
                if (len(ad['FinalUrls'][0]) > 0):
                    finalUrl = ad['FinalUrls'][0][0]
                else:
                    finalUrl = ''
                insertion_query += '(' + dwh_date + ',\'bing_ads\',\'' + str(ad['Id']) + '\',\'' + str(ad['Id'])
                insertion_query += '\',\'' + ad['Status'] + '\',\'' + finalUrl + '\',\'' + ad['Type'] + '\',\'' + str(adgroup_id) + '\'),'
        except Exception as e:
            pass
    try:
        insertion_query = insertion_query[:-1] #removes the last comma
        insertion_query += ';'
        con = pc.connect(dbname = db.credentials['db_name'] , host = db.credentials['db_host'] , port = db.credentials['db_port'], user = db.credentials['db_user'], password = db.credentials['db_pw'])
        cur = con.cursor()
        cur.execute(insertion_query)
        con.commit()
    except Exception as e:
        pass
