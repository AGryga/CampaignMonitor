import requests
from pandas.io.json import json_normalize
import pandas as pd
import numpy as np

#Enter the API Key below for your Campaign Monitor Account
#apikey = ''
#client_id = ''
#list_id = ''


#Retrieves client(s) associated to account
def get_clients():
    r = requests.get('https://api.createsend.com/api/v3.2/clients.json', auth=(apikey, '')).json()

    print('You have the following clients associated to your account. \n')
    for client in r:
        print(client.get('Name') + ' : ' + client.get('ClientID'))


#Retrieves all subscription lists for respective client ID
def get_subscription_lists(client_id):
    print(f'You have the following lists available for client_id {client_id}. \n')

    #Option below to retrieve as dataframe 
    #sub_list_df = json_normalize(requests.get(f'https://api.createsend.com/api/v3.2/clients/{client_id}/lists.json', auth = (apikey, '')).json())  
    r = requests.get(f'https://api.createsend.com/api/v3.2/clients/{client_id}/lists.json', auth = (apikey, '')).json()
    
    for sub_list in r:
        print(sub_list.get('Name') + ' : ' + sub_list.get('ListID'))


#Retrieves the list of subscribers within a subscription list
def retrieve_subscriber_list(list_id):
    r = requests.get(f'https://api.createsend.com/api/v3.2/lists/{list_id}/active.json', auth = (apikey, '')).json()
    #Retrieve number of pages within subscriber list queried
    page_count = r.get('NumberOfPages')

    #Creates master patient DF to recreate subscriber's list provided to email manager
    subscribers = pd.DataFrame()

    #Cycles through all pages of the subscriber list, retrieving the max of 1000 subscribers per page.
    for page in range(1, (page_count + 1)):

        #Need to loop through each individual page within the global list, each page containing 1,000 subscribers.
        payload_json = requests.get(f'https://api.createsend.com/api/v3.2/lists/{list_id}/active.json?page={page}&pagesize=1000', auth = (apikey, '')).json()
        
        #Retrieves the email address, name and all associated custom fields with a subscriber.
        payload = json_normalize(payload_json['Results'], record_path=['CustomFields'], 
                                meta = ['EmailAddress', 'Name'], errors = 'ignore')
        
        #Reshape the JSON data into the standard dataframe
        payload_df_formatted = payload.pivot_table(columns='Key', values='Value', index=['EmailAddress', 'Name'], aggfunc='sum')

        #Retrieves each page containing 1000 subscribers and appends the results into a master df containing all pages.
        subscribers = pd.concat([subscribers, payload_df_formatted], axis = 0)

        print(f'Page {page} added')

    #Resets index to move EmailAddress and Name from the index into columns. Cleans up column formatting.
    subscribers.reset_index(inplace=True, drop = False)
    subscribers.columns = subscribers.columns.str.strip('[]') 

    print('Subscriber list created.')



def upload_subscriber_list(list_id, df):
    #Ensures that no null values exist within the dataframe provided.
    df.replace(np.nan, '', regex = True, inplace = True)

    #If you do not have a ConsentToTrack column, please use the line below
    #df['ConsentToTrack'] = 'Yes'

    #Retrieves number of pages needed to upload all subscribers (1000 subscribers per page)
    upload_pages = len(df.index) / 1000

    #Grabs 1000 subscribers, transforms dataframe into required JSON format for upload.
    for value in range(0, upload_pages + 1):
        sub_count = value * 1000
            
        df = df.iloc[sub_count : sub_count+1000, :].groupby(['EmailAddress', 'Name', 'ConsentToTrack']).apply(lambda x : x[[x for x in df.columns if x not in ['EmailAddress', 'Name', 'ConsentToTrack']]].to_dict(orient = 'records'))
        df = df.reset_index().rename(columns = {0 : 'CustomFields'})

        df['CustomFields'] = df['CustomFields'].apply(lambda x : [{'Key' : f'[{k}]', 'Value' : v} for k, v in x[0].items()])

        json_payload = df[['EmailAddress', 'Name', 'ConsentToTrack', 'CustomFields']].to_json(orient = 'records')
        json_payload = '{"Subscribers" : ' + json_payload + '}'
        
        response = requests.post(f'https://api.createsend.com/api/v3.2/subscribers/{listid_value}/import.json', json = json.loads(json_payload), auth = (apikey, ''))
        print("Status code: ", response.status_code)
        print(response.json())
        print(f'Page {value} uploaded.')

    print('Subscription list successfully updated.')


#Creates reporting dataframe containign all results for campaigns
def reporting(client_id):
    r = requests.get(f'https://api.createsend.com/api/v3.2/clients/{client_id}/campaigns.json', auth = (apikey, '')).json()

    campaign_overview = json_normalize(r, errors = 'ignore')
    campaign_overview.drop(columns = ['FromName', 'FromEmail', 'ReplyTo', 'WebVersionURL', 'WebVersionTextURL'], inplace = True)

    campaign_stats = pd.DataFrame()

    #Collects email performance for each campaign
    for campaign in campaign_overview['CampaignID']:
        campaign_data = requests.get(f'https://api.createsend.com/api/v3.2/campaigns/{campaign}/summary.json', auth=(apikey, '')).json()
        campaign_payload = json_normalize(campaign_data, errors = 'ignore')
        campaign_payload = campaign_payload[['TotalOpened', 'UniqueOpened', 'Clicks', 'Unsubscribed', 'Bounced', 'SpamComplaints', 'Forwards', 'Likes', 'Mentions']]
        campaign_payload.insert(0, 'CampaignID', campaign)
        campaign_stats = pd.concat([campaign_stats, campaign_payload], axis = 0)

    campaign_overview = campaign_overview.merge(campaign_stats, on='CampaignID')

    print('Successfully retrieved performance for all campaigns.')