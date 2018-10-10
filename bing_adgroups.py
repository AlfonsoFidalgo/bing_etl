import datetime
import psycopg2 as pc
import db_credentials as db



def insert_adgroup_details(campaign_ids, campaignmanagement_service):
    for campaign_id in campaign_ids:
        response = campaignmanagement_service.GetAdGroupsByCampaignId(
            CampaignId = campaign_id,
            ReturnAdditionalFields = [],
            ReturnCoOpAdGroups = False)
