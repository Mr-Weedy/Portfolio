import pandas as pd
import numpy as np


bank_marketing_df = pd.read_csv('bank_marketing.csv')

# Creating dataframes to hold the data for the different output files.
client_df = bank_marketing_df[['client_id', 'age', 'job', 'marital', 'education', 
                               'credit_default', 'mortgage']]
campaign_df = bank_marketing_df[['client_id', 'number_contacts', 
                                 'contact_duration', 'previous_campaign_contacts', 
                                 'previous_outcome', 'campaign_outcome']]
economics_df = bank_marketing_df[['client_id', 'cons_price_idx', 
                                  'euribor_three_months']]


# Cleaning data in the client dataframe. 
client_df['job'] = client_df['job'].str.replace('.', '_')
assert len(client_df[client_df['job'].str.find('.') > -1]) == 0

client_df['education'] = client_df['education'].str.replace('.', '_')
assert len(client_df[client_df['education'].str.find('.') > -1]) == 0

client_df['education'].replace('unknown', np.NaN, inplace=True)
assert len(client_df[client_df['education'].str.find('unknown') > -1]) == 0

client_df['credit_default'].replace({'yes': True, 'no': False, 'unknown': False}, 
                                 inplace=True)
assert client_df['credit_default'].dtype == 'bool'

client_df['mortgage'].replace({'yes': True, 'no': False, 'unknown': False}, 
                           inplace=True)
assert client_df['mortgage'].dtype == 'bool'

# Cleaning data in the campaign dataframe. 
campaign_df['previous_outcome'].replace({'success': True, 'failure': False, 
                                      'nonexistent': False}, inplace=True)
assert campaign_df['previous_outcome'].dtype == 'bool'

campaign_df['campaign_outcome'].replace({'yes': True, 'no': False}, inplace=True)
assert campaign_df['campaign_outcome'].dtype == 'bool'

repl_month = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6, 
              'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 
              'nov': 11, 'dec': 12}
campaign_df['last_contact_date'] = pd.to_datetime(
    {'year': '2022', 'month': bank_marketing_df['month'].replace(repl_month), 
     'day': bank_marketing_df['day']}, format='%Y-%m-%d')

# Exporting the dataframes to .csv files. 
client_df.to_csv('client.csv', index=False)
campaign_df.to_csv('campaign.csv', index=False)
economics_df.to_csv('economics.csv', index=False)
