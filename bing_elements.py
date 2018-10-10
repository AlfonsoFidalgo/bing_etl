from auth_helper import *
from output_helper import *

import bing_accounts as accounts
import bing_campaigns as campaigns
import bing_adgroups as adgroups
import bing_adcopy as ads

##AUTHORIZE
authorization_data=AuthorizationData(
    account_id=None,
    customer_id=None,
    developer_token=DEVELOPER_TOKEN,
    authentication=None,
)

authenticate(authorization_data)

##CALL DIFFERENT SERVICES
#Account Service
customermanagement_service = ServiceClient(
    'CustomerManagementService',
    authorization_data = authorization_data,
    environment = ENVIRONMENT,
    version=11,
)
#Campaign Management Service (campaign, adgroup, keywords, ads)
campaignmanagement_service = ServiceClient(
    'CampaignManagementService',
    authorization_data = authorization_data,
    environment = ENVIRONMENT,
    version = 11,
)

##API account response
account_response = accounts.get_api_response(customermanagement_service)
#Account IDs accesible in this list
account_ids = accounts.get_account_ids(account_response)

##Account details are uploaded to the db table
def upload_accounts():
    accounts.insert_account_details(account_response)
    print('finished updating accounts')

def upload_campaigns():
    campaigns.delete_campaigns()
    for account_id in account_ids:
        authenticate(authorization_data, account_id)
        campaignmanagement_service = ServiceClient(
            'CampaignManagementService',
            authorization_data = authorization_data,
            environment = ENVIRONMENT,
            version = 11,
        )
        print('finished updating campaigns')
        campaigns.insert_campaign_details([account_id], campaignmanagement_service)

def upload_adgroups():
    adgroups.delete_adgroups()
    for account_id in account_ids:
        authenticate(authorization_data, account_id)
        campaignmanagement_service = ServiceClient(
            'CampaignManagementService',
            authorization_data = authorization_data,
            environment = ENVIRONMENT,
            version = 11,
        )
        campaign_ids = campaigns.get_campaign_ids([account_id], campaignmanagement_service)
        adgroup_ids = adgroups.insert_adgroup_details(campaign_ids, campaignmanagement_service)
        print('finished updating adgroups')
        return adgroup_ids

def upload_ads(adgroup_ids):
    ads.delete_ads()
    for account_id in account_ids:
        authenticate(authorization_data, account_id)
        campaignmanagement_service = ServiceClient(
            'CampaignManagementService',
            authorization_data = authorization_data,
            environment = ENVIRONMENT,
            version = 11,
        )



def upload_keywords():
    pass


# if __name__ == '__main__':
#     #upload_accounts()
