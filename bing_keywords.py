import datetime
import psycopg2 as pc
import db_credentials as db


def delete_keywords():
    deletion_query = 'DELETE FROM heycar.mkt_keyword  WHERE mkt_source=\'bing_ads\';'
    con = pc.connect(dbname = db.credentials['db_name'] , host = db.credentials['db_host'] , port = db.credentials['db_port'], user = db.credentials['db_user'], password = db.credentials['db_pw'])
    cur = con.cursor()
    cur.execute(deletion_query)
    con.commit()


def insert_keyword_details(adgroup_ids, campaignmanagement_service):
    insertion_query = 'INSERT INTO heycar.mkt_keyword (dwh_date, mkt_source, keyword_id, text, match_type, adgroup_id) VALUES '
    dwh_date = datetime.datetime.today().strftime('%Y-%m-%d')
    dwh_date = "'" + str(dwh_date) + "'"

    for adgroup_id in adgroup_ids:
        try: #adgroup_id might be from a different account
            response = campaignmanagement_service.GetKeywordsByAdGroupId(AdGroupId = adgroup_id)
            for kw in response[0]:
                insertion_query +='(' + dwh_date + ',\'bing_ads\',\'' + str(kw['Id']) + '\',\'' + kw['Text'] + '\',\'' + kw['MatchType'] + '\',\'' + str(adgroup_id) + '\'),'
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
