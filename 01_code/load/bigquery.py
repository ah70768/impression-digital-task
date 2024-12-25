import datetime, time
import pandas as pd
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

from  extract.fetch_shopify_data import ShopifyAPI 

class BigQueryManager:

    def __init__(self, project_id, credentials_path=None):
        """
        Initializes the BigQueryManager with the given project ID and credentials.

        Args:
            project_id (str): The GCP project ID.
            credentials_path (str, optional): The path to the service account JSON key file (needed if testing locally) 
                                              If None, the default application credentials are used.
        """
        
        self.project_id = project_id
        self.credentials_path = credentials_path
        
        if credentials_path is not None:
            self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        else:
            self.credentials = None
        
        self.client = bigquery.Client(project=self.project_id, credentials = self.credentials)
        

    
    def create_dataset(self, dataset_id, location = 'EU'):
        
        """
        Creates a dataset in the specified project.

        Args:
            dataset_id (str): The ID of the dataset to create.
            location (str, optional): The geographic location for the dataset (default is 'EU').

        Returns:
            None
        """
        
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
        
        return
    

    def load_table(self, dataframe, dataset_id, table_id, if_exists='replace'):

        """
        Loads data from a pandas DataFrame into a BigQuery table.

        Args:
            dataframe (pandas.DataFrame): The data to load into BigQuery.
            dataset_id (str): The name of the dataset where the table resides.
            table_id (str): The name of the table to load data into.
            if_exists (str, optional): Defines behavior if the table already exists. Default is 'replace', alternative is 'append'.

        Returns:
            None
        """

        table_ref = self.client.dataset(dataset_id).table(table_id)
        if if_exists == 'replace':
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", source_format=bigquery.SourceFormat.PARQUET)
        elif if_exists == 'append':
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", source_format=bigquery.SourceFormat.PARQUET)
        
        try:        
            job = self.client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
            job.result()
            print('Data loaded into Table: {t_id} in Dataset: {d_id}'.format(t_id=table_id,d_id=dataset_id))
        except Exception as e:
            print('An error: {error} occurred whilst loading the data: {t_id}'.format(t_id=table_id, error=e))

        return


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
    
