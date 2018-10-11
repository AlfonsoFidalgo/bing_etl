from auth_helper import *
from output_helper import *
import pandas as pd
import bing_accounts as accounts

FILE_DIRECTORY='./'
DOWNLOAD_FILE_NAME='bing_api_report.csv'
REPORT_FILE_FORMAT='Csv'
TIMEOUT_IN_MILLISECONDS=3600000

def main(authorization_data):
    try:
        report_request=get_keyword_performance_report_request()
        reporting_download_parameters = ReportingDownloadParameters(
            report_request=report_request,
            result_file_directory = FILE_DIRECTORY,
            result_file_name = DOWNLOAD_FILE_NAME,
            overwrite_result_file = True,
            timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS
        )
        output_status_message("Awaiting Background Completion . . .");
        background_completion(reporting_download_parameters)
        output_status_message("Program execution completed")

    except WebFault as ex:
        output_webfault_errors(ex)
    except Exception as ex:
        output_status_message(ex)

def background_completion(reporting_download_parameters):
    global reporting_service_manager
    result_file_path = reporting_service_manager.download_file(reporting_download_parameters)

def get_keyword_performance_report_request():
    report_request=reporting_service.factory.create('KeywordPerformanceReportRequest')
    report_request.Format=REPORT_FILE_FORMAT
    report_request.ReportName='Keyword Performance Report'
    report_request.ReturnOnlyCompleteData=False
    report_request.Aggregation='Daily'
    report_request.Language='English'

    scope=reporting_service.factory.create('AccountThroughAdGroupReportScope')

    scope.AccountIds={'long': account_ids }
    scope.Campaigns=None
    scope.AdGroups=None
    report_request.Scope=scope

    report_time=reporting_service.factory.create('ReportTime')

    report_time.PredefinedTime='LastSevenDays'   #'Yesterday'
    report_request.Time=report_time

    report_columns=reporting_service.factory.create('ArrayOfKeywordPerformanceReportColumn')
    report_columns.KeywordPerformanceReportColumn.append([
        'TimePeriod',
        'AccountId',
        'CampaignId',
        'CampaignName',
        'AdGroupId',
        'AdGroupName',
        'AdId',
        'KeywordId',
        'BidMatchType',
        'Keyword',
        'AveragePosition',
        'Clicks',
        'Spend',
        'Impressions',
        'QualityScore'
    ])
    report_request.Columns=report_columns
    return report_request

def process_file():
    df = pd.read_csv('bing_api_report.csv', sep=',', header=9)
    df.dropna(inplace=True)
    df['mkt_source'] = 'bing_ads'
    df['Impressions'] = df['Impressions'].apply(lambda x: int(x))
    df['Clicks'] = df['Clicks'].apply(lambda x: int(x))
    df['CampaignId'] = df['CampaignId'].apply(lambda x: int(x))
    df['AdGroupId'] = df['AdGroupId'].apply(lambda x: int(x))
    df['AccountId'] = df['AccountId'].apply(lambda x: int(x))
    df['AdId'] = df['AdId'].apply(lambda x: int(x))
    df['KeywordId'] = df['KeywordId'].apply(lambda x: int(x))
    df[df['Impressions'] > 0].to_csv('bing_api_report_processed.csv',index=False)


if __name__ == '__main__':
authorization_data=AuthorizationData(
        account_id=None,
        customer_id=None,
        developer_token=DEVELOPER_TOKEN,
        authentication=None,
)

reporting_service_manager=ReportingServiceManager(
        authorization_data=authorization_data,
        poll_interval_in_milliseconds=5000,
        environment=ENVIRONMENT,
)

reporting_service=ServiceClient(
        'ReportingService',
        authorization_data=authorization_data,
        environment=ENVIRONMENT,
        version=11,
)

authenticate(authorization_data)

customermanagement_service = ServiceClient(
        'CustomerManagementService',
        authorization_data = authorization_data,
        environment = ENVIRONMENT,
        version=11,
)

account_response = accounts.get_api_response(customermanagement_service)
account_ids = accounts.get_account_ids(account_response)
main(authorization_data)
process_file()
