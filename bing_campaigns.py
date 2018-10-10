import datetime
import psycopg2 as pc
import db_credentials as db



def get_api_response(campaignmanagement_service, account_id):
    """
    returns campaign info response from Bing API
    """
    response = campaignmanagement_service.GetCampaignsByAccountId(
        AccountId = account_id,
        CampaignType = 'SearchAndContent',
        ReturnCoOpCampaigns = False
    )
    return response

def get_campaign_ids(account_ids, campaignmanagement_service):
    """
    Returns list of campaign Ids given a list of account ids
    """
    campaign_ids = []
    for account_id in account_ids:
        try:
            response = get_api_response(campaignmanagement_service, account_id)
            for campaign in response[0]:
                campaign_ids.append(campaign['Id'])
        except Exception as e:
            print('No campaigns found in account id ' + account_id)
    return campaign_ids
        #return [campaign['Id'] for campaign in response[0]]

def insert_campaign_details(account_ids, campaignmanagement_service):
    """
    Inserts Bing campaign details into heycar.mkt_campaign
    """
    deletion_query = 'DELETE FROM heycar.mkt_campaign  WHERE mkt_source=\'bing_ads\';'
    insertion_query = 'INSERT INTO heycar.mkt_campaign(dwh_date, mkt_source, campaign_id, name, status, network, cost_limit, account_id) VALUES '
    dwh_date = datetime.datetime.today().strftime('%Y-%m-%d')
    dwh_date = "'" + str(dwh_date) + "'"

    for account_id in account_ids:
        #response is the list of campaigns of each account
        try:
            response = get_api_response(campaignmanagement_service, account_id)
            for campaign in response[0]:
                insertion_query +='(' + dwh_date + ',\'bing_ads\',\'' + str(campaign['Id']) + '\',\'' + campaign['Name'] + '\',\'' + campaign['Status'] + '\',\'' + 'SEARCH' + '\',\'' + str(campaign['DailyBudget']) + '\',\'' + str(account_id) + '\'),'
            insertion_query = insertion_query[:-1]
        except Exception as e:
            print("error fetching campaigns: " + str(e))
    insertion_query += ';'
    # print(insertion_query)
    con = pc.connect(dbname = db.credentials['db_name'] , host = db.credentials['db_host'] , port = db.credentials['db_port'], user = db.credentials['db_user'], password = db.credentials['db_pw'])
    cur = con.cursor()
    cur.execute(deletion_query)
    con.commit()
    cur.execute(insertion_query)
    con.commit()
