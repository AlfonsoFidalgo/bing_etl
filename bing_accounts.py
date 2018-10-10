import datetime
import psycopg2 as pc
import db_credentials as db


def get_api_response(customermanagement_service):
    """
    returns accounts info response from Bing API
    """
    response = customermanagement_service.GetAccountsInfo(
        CustomerId='250177243',
        OnlyParentAccounts=False)
    return response


def get_account_ids(response):
    """
    Returns list of accounts Ids
    """
    return [account['Id'] for account in response[0]]

def insert_account_details(response):
    """
    Inserts Bing account details into heycar.mkt_account
    """
    deletion_query = 'DELETE FROM heycar.mkt_account  WHERE mkt_source=\'bing_ads\';'
    insertion_query = 'INSERT INTO heycar.mkt_account(dwh_date, mkt_source, account_id, name) VALUES '
    dwh_date = datetime.datetime.today().strftime('%Y-%m-%d')
    dwh_date = "'" + str(dwh_date) + "'"
    for account in response[0]:
        #should INSERT INTO heycar.mkt_account
        insertion_query += '(' + dwh_date + ',\'bing_ads\',\'' + str(account['Id']) + '\', \'' + account['Name'] +'\'),'
    insertion_query = insertion_query[:-1]
    insertion_query += ';'
    print(insertion_query)
    con = pc.connect(dbname = db.credentials['db_name'] , host = db.credentials['db_host'] , port = db.credentials['db_port'], user = db.credentials['db_user'], password = db.credentials['db_pw'])
    cur = con.cursor()
    cur.execute(deletion_query)
    con.commit()
    cur.execute(insertion_query)
    con.commit()
