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
    pass
