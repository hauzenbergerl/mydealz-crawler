import os
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import datetime
import time
import re
import google.cloud.bigquery as bq
from google.cloud.exceptions import NotFound

def get_soup(url, parser='lxml'):
    source = requests.get(url).text
    return(BeautifulSoup(source, parser))

def get_all_href(soup, filter_string):
    out_list = []
    for a in soup.find_all('a', href=True):
        temp = a['href']
        if filter_string in temp:
            temp = temp.replace('#comments', '')
            out_list.append(temp)
    return(list(dict.fromkeys(out_list)))

def get_meta_property(soup, property):
    meta_property = soup.find('meta',  {'property': property})['content']
    return(meta_property)

def get_temp(soup):
    temperature = soup.find('span', {'class': 'cept-vote-temp'})
    if temperature is not None:
        temperature = temperature.get_text().replace('\n', '').replace('\t', '').replace(chr(176), '')
        expired = 'Nein'
    else:
        try:
            temp_soup = soup.find('svg', {'class': 'icon icon--hourglass'}).parent
            test_str = temp_soup.get_text()
            if 'abgelaufen' in test_str.lower():
                temperature = re.search(r'[^\u00B0][0-9]+', test_str).group(0)
                temperature = re.sub('[^0-9]', '', temperature)
                expired = 'Ja'
        except (AttributeError, IndexError):
            temperature = 'not found'
            expired = 'not found'
    return [temperature, expired]

def get_price(soup):
    element_list =  soup.find('h1', {'class': 'thread-title'}).parent.findChildren('span')
    price_list = []
    for element in element_list:
        element_text = element.get_text()
        if '€' in element_text and '%' not in element_text:
            regex_string = re.search(r'[^A-Za-z]', element_text).group(0).strip()
            no_letters_check = True if len(regex_string) > 0 else False
            if no_letters_check:
                price_list.append(element_text.replace('€', '').replace('.', '').replace(',', '.'))
        elif 'kostenlos' in element_text.lower():
            price_list.append(0)
    try:
        price_list = [float(price) for price in price_list]
        if len(price_list) > 2:
            price_list = list(set(price_list))
        price_list.sort()
        price_list = price_list[:2]
        if len(price_list) != 2:
            price_list = [np.nan, np.nan]
    except (TypeError, ValueError):
        price_list = [np.nan, np.nan]
    return(price_list)

def get_merchant(soup):
    sub_soup = soup.find('h1', {'class': 'thread-title'}).parent
    try:
        merchant = sub_soup.select('span[class*="merchant"]')[1].get_text()
    except IndexError:
        merchant = 'not found'
    return(merchant)

def generate_key(key_fields):
    key_list = []
    for field in key_fields:
        if isinstance(field, datetime.date):
            key_part = int(time.mktime(field.timetuple()))
        elif isinstance(field, str):
            key_part = abs(hash(field)) % (10 ** 8)
            key_part = str(key_part).rjust(8, '0')
        else:
            key_part = field
        key_list.append(str(key_part))
    key = '_'.join(key_list)
    return(key)

def doesTableExist(bigquery_client, table_ref):
        try:
            table = bigquery_client.get_table(table_ref)
            if table:
                return True
        except NotFound as error:
            return False

def create_insert_table(df, project_name, dataset_name, table_name):
    # set refs
    client = bq.Client(project='v135-4542-pricing-cockpit-dev')
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    # upload data to bq
    print('starting data upload')
    if doesTableExist(client, table_ref):
        table_ref_temp = dataset_ref.table(table_name+'_temp')
        client.load_table_from_dataframe(df, table_ref_temp).result()
        query = """INSERT """ + table_ref.dataset_id + """.""" + table_ref.table_id + """
        SELECT * FROM """ + table_ref_temp.dataset_id + """.""" + table_ref_temp.table_id
        client.query(query).result()
        client.delete_table(table_ref_temp)
    else:
        client.load_table_from_dataframe(df, table_ref).result()
    print('upload complete')