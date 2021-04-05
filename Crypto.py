
import requests
import yaml
import boto3
import sqlalchemy as db
import pandas as pd
from datetime import datetime
import uuid

# Load Credential YAML-File
credentials = yaml.load(open('credentials.yaml'), Loader=yaml.FullLoader)

# Set Api Data to Dict
api_data = {
    'host': 'https://pro-api.coinmarketcap.com',
    'map': '/v1/cryptocurrency/map',
    'quotes': '/v1/cryptocurrency/quotes/latest'
}

headers = {
    'X-CMC_PRO_API_KEY': credentials['coin']['key']
}

params = {
    'symbol': 'BNB,BTC,ETH'
}

# Date Data
now = datetime.now()
dt_string = now.strftime('%Y/%m/%d/%H/%M')


# Build Url for request
def build_url(host, endpoint):
    url = host+endpoint
    return url


url = build_url(api_data['host'], api_data['quotes'])

r = requests.get(url, headers=headers, params=params)
response = r.json()


# Store local
'''
with open('coin_qoutes.json', 'w') as json_file:
    json.dump(response, json_file)
    '''

# AWS S3 Connection
s3 = boto3.client('s3', aws_access_key_id=credentials['aws']['key'], aws_secret_access_key=credentials['aws']['secret'])

with open('coin_qoutes.json', 'rb') as json_file:
    s3.upload_fileobj(json_file, 'coinmarketcap-data', f'{dt_string}/coin_qoutes.json')

# AWS RDS Postgres
db_user = credentials['db']['user']
db_pass = credentials['db']['password']
db_host = credentials['db']['host']
db_database = credentials['db']['database']

engine = db.create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_database}')


# Build Dataframe Statement for SQL Insert
def build_stmt(response):

    df_coins = pd.DataFrame()

    for i in response['data']:
        coin = response['data'][i]
        coin.pop('platform', None)
        data_norm = pd.json_normalize(coin)
        data_norm['uuid'] = uuid.uuid4()
        df_coins = df_coins.append(data_norm)

    return df_coins


coin_data = build_stmt(response)

# Insert into SQl Database
coin_data.to_sql(name='coinqoutes', con=engine, if_exists='append', method='multi')
