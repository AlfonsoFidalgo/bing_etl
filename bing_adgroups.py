import datetime
import psycopg2 as pc
import db_credentials as db


def delete_adgroups():
    deletion_query = 'DELETE FROM heycar.mkt_adgroup  WHERE mkt_source=\'bing_ads\';'
    con = pc.connect(dbname = db.credentials['db_name'] , host = db.credentials['db_host'] , port = db.credentials['db_port'], user = db.credentials['db_user'], password = db.credentials['db_pw'])
    cur = con.cursor()
    cur.execute(deletion_query)
    con.commit()


def insert_adgroup_details(campaign_ids, campaignmanagement_service):
    insertion_query = 'INSERT INTO heycar.mkt_adgroup(dwh_date,mkt_source, adgroup_id, name, status, campaign_id) VALUES '
    dwh_date = datetime.datetime.today().strftime('%Y-%m-%d')
    dwh_date = "'" + str(dwh_date) + "'"
    adgroup_ids = []
    for campaign_id in campaign_ids:
        response = campaignmanagement_service.GetAdGroupsByCampaignId(
            CampaignId = campaign_id,
            ReturnAdditionalFields = [],
            ReturnCoOpAdGroups = False)
        for adgroup in response[0]:
            adgroup_ids.append(adgroup['Id'])
            insertion_query +='(' + dwh_date + ',\'bing_ads\',\'' + str(adgroup['Id']) + '\',\'' + adgroup['Name'] + '\',\'' + adgroup['Status'] + '\',\'' + str(campaign_id) + '\'),'
    insertion_query = insertion_query[:-1] #removes the last comma
    con = pc.connect(dbname = db.credentials['db_name'] , host = db.credentials['db_host'] , port = db.credentials['db_port'], user = db.credentials['db_user'], password = db.credentials['db_pw'])
    cur = con.cursor()
    cur.execute(insertion_query)
    con.commit()
    return adgroup_ids
