import datetime, time
import pandas as pd
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# used for testing purposes in __name__ == '__main__' block
# from  extract.fetch_shopify_data import ShopifyAPI 

class BigQueryManager:

    def __init__(self, project_id, credentials_path):
        
        self.project_id = project_id
        self.credentials_path = credentials_path

        self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.client = bigquery.Client(project=self.project_id, credentials = self.credentials)

    
    def create_dataset(self, dataset_id, location = 'EU'):
        
        dataset_ref = bigquery.DatasetReference(self.project_id, dataset_id) 
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location

        try:
            self.client.create_dataset(dataset, timeout=30)
            print('Dataset: {ds} created in Project: {pid}'.format(ds=dataset_id, pid=self.project_id))
        except Exception as e:
            if 'Already Exists' in str(e):
                print('Dataset: {ds} already exists in Project: {pid}'.format(ds=dataset_id, pid=self.project_id))
            else:
                print('An error occured {error}'.format(error=e))
    

    def load_table(self, dataframe, dataset_id, table_id, if_exists='replace'):

        table_ref = self.client.dataset(dataset_id).table(table_id)

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", source_format=bigquery.SourceFormat.PARQUET)
        job = self.client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
        job.result()
        print('Data loaded into Table: {t_id} in Dataset: {d_id}'.format(t_id=table_id,d_id=dataset_id))


if __name__ == '__main__':
    
    # dotenv_path = os.path.join(os.path.dirname(__file__), "..", "config", ".env")
    # load_dotenv(dotenv_path)

    # shop = os.getenv('SHOP_URL')
    # api_key = os.getenv('SHOPIFY_ADMIN_API_ACCESS_TOKEN')
    # api_version = os.getenv('SHOPIFY_API_VERSION')

    # client = ShopifyAPI(shop, api_key, api_version)
    # client.create_session()
        
    # # table_names = ['Order', 'Product', 'Customer']
    # table_names =  ['Variants']
    # tables_df = client.fetch_tables(table_names)

    # project_id = os.getenv('GCP_PROJECT_ID')
    # gcp_creds_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'impression-digital-22a0eecacc9a.json')

    # # print(project_id)
    # bq = BigQueryManager(project_id, gcp_creds_path)

    # # create datasets for each stage of data processing
    # datasets = ['raw', 'staging', 'processed']
    # for ds in datasets:
    #     bq.create_dataset(ds)

    # for key in tables_df.keys():
    #     bq.load_table(tables_df[key], 'raw', key.lower())
    
    pass
    
