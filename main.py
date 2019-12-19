import os
import pandas as pd
import datetime
import utils
#from fuzzywuzzy import fuzz

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/hauzenbergerl/Documents/fcyvtaposcsd-d381dbe0e39e.json'

url = 'https://www.mydealz.de'

# get deals from main page
deals_list_mp = utils.get_all_href(utils.get_soup(url), url+'/deals/')
df_deals = pd.DataFrame(deals_list_mp, columns=['url'])
df_deals['gruppe'] = 'hauptseite'

# extract groups
cat_url = url+'/gruppe/'
cat_list = utils.get_all_href(utils.get_soup(cat_url), cat_url)
cat_list = [cat for cat in cat_list if '/hub/' not in cat]
cat_df = pd.DataFrame(cat_list, columns=['url'])
cat_df['gruppe'] = cat_df['url'].apply(lambda x: x.split('/')[-1])

#get deals from group pages
relevant_groups = ['laptop', 'fernseher', 'gaming', 'home-living', 'telefon-internet']
cat_df = cat_df[cat_df['gruppe'].isin(relevant_groups)]
for gruppe in cat_df['gruppe']:
    temp_url = cat_df[cat_df['gruppe'] == gruppe]['url'].iloc[0]
    temp_list = utils.get_all_href(utils.get_soup(temp_url), url+'/deals/')
    temp_df = pd.DataFrame(temp_list, columns=['url'])
    temp_df['gruppe'] = gruppe
    df_deals = df_deals.append(temp_df, ignore_index=True, sort=False)

# drop duplicates for articles which are on the main page and on a group page
df_deals = df_deals.drop_duplicates(subset=['url'])

#get soups for article pages
df_deals['soup'] = df_deals['url'].apply(lambda x: utils.get_soup(x))
df_deals['extrahiert'] = datetime.datetime.now()

#get additional information from soup
df_deals['veroeffentlicht'] = df_deals['soup'].apply(lambda x: utils.get_meta_property(x, 'og:article:published_time'))
df_deals['veroeffentlicht'] = df_deals['veroeffentlicht'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
df_deals['name'] = df_deals['url'].apply(lambda x: x.split('/')[-1])
df_deals[['temperatur', 'abgelaufen']] = df_deals['soup'].apply(lambda x: pd.Series(utils.get_temp(x)))
df_deals[['preis_neu', 'preis_alt']] = df_deals['soup'].apply(lambda x: pd.Series(utils.get_price(x)))
df_deals['haendler'] = df_deals['soup'].apply(lambda x: utils.get_merchant(x))
df_deals.index = df_deals.apply(lambda row: utils.generate_key(row[['extrahiert', 'veroeffentlicht', 'name', 'gruppe']]), axis=1)
df_deals.index.name = 'KEY'

# move soup field to seperate df
df_soup = pd.DataFrame(df_deals['soup'].astype(str).copy())
df_deals.drop(['soup'], axis=1, inplace=True)

# TODO import and match MMS products
# filename = 'prod_all_20191215_DE.csv'
# filepath = os.getcwd() + '\\' + filename
# prod_all = pd.read_csv(filepath)
#match prod names

#upload data
project_name = 'v135-4542-pricing-cockpit-dev'
dataset_name = 'ad_hoc'
table_name_deals = 'mydealz_data'
table_name_soup = 'mydealz_data_html'

# df_deals
utils.create_insert_table(df_deals, project_name, dataset_name, table_name_deals)
# df_soup
utils.create_insert_table(df_soup, project_name, dataset_name, table_name_soup)