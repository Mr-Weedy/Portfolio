import pandas as pd
import numpy as np


bank_marketing_df = pd.read_csv('bank_marketing.csv')

client = bank_marketing_df[['client_id', 'age', 'job', 'marital', 'education', 
                            'credit_default', 'mortgage']]
campaign = bank_marketing_df[['client_id', 'number_contacts', 'contact_duration', 
                              'previous_campaign_contacts', 'previous_outcome', 
                              'campaign_outcome']]
economics = bank_marketing_df[['client_id', 'cons_price_idx', 
                               'euribor_three_months']]


client['job'] = client['job'].str.replace('.', '_')
assert len(client[client['job'].str.find('.') > -1]) == 0

client['education'] = client['education'].str.replace('.', '_')
assert len(client[client['education'].str.find('.') > -1]) == 0

client['education'].replace('unknown', np.NaN, inplace=True)
assert len(client[client['education'].str.find('unknown') > -1]) == 0

client['credit_default'].replace({'yes': True, 'no': False, 'unknown': False}, 
                                 inplace=True)
assert client['credit_default'].dtype == 'bool'

client['mortgage'].replace({'yes': True, 'no': False, 'unknown': False}, 
                           inplace=True)
assert client['mortgage'].dtype == 'bool'

campaign['previous_outcome'].replace({'success': True, 'failure': False, 
                                      'nonexistent': False}, inplace=True)
assert campaign['previous_outcome'].dtype == 'bool'

campaign['campaign_outcome'].replace({'yes': True, 'no': False}, inplace=True)
assert campaign['campaign_outcome'].dtype == 'bool'

repl_month = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6, 
              'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 
              'nov': 11, 'dec': 12}
campaign['last_contact_date'] = pd.to_datetime(
    {'year': '2022', 'month': bank_marketing_df['month'].replace(repl_month), 
     'day': bank_marketing_df['day']}, format='%Y-%m-%d')

client.to_csv('client.csv', index=False)
campaign.to_csv('campaign.csv', index=False)
economics.to_csv('economics.csv', index=False)
