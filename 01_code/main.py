import os
import subprocess
import yaml
from extract.fetch_shopify_data import ShopifyAPI
from load.bigquery import BigQueryManager


class Pipeline:

    def __init__(self, config_path, local_deployment=True):

        self.config_path = config_path

        self.load_environment()

        if local_deployment:
            self.bigquery = BigQueryManager(project_id=os.getenv('GCP_PROJECT_ID'), 
                                            credentials_path=os.path.join(config_path, os.getenv('GCP_CREDENTIALS'))
            )
        else:
            self.bigquery = BigQueryManager(project_id=os.getenv('GCP_PROJECT_ID'))

        self.shopify_client = ShopifyAPI(shop=os.getenv('SHOP_URL'), 
                                         api_key=os.getenv('SHOPIFY_ADMIN_API_ACCESS_TOKEN'), 
                                         api_version=os.getenv('SHOPIFY_API_VERSION')
        )
        
        self.datasets = ['raw', 'staging', 'processed']
        self.table_names = ['Order', 'Product', 'Customer', 'Variant']

    
    def load_environment(self):

        yaml_path = os.path.join(self.config_path, 'env.yaml')      

        with open(yaml_path, 'r') as file:
            config = yaml.safe_load(file) 
        
        for key, value in config.items():
            os.environ[key] = value 
        return

    
    def fetch_data(self):
        
        self.shopify_client.create_session()
        tables_df = self.shopify_client.fetch_tables(self.table_names)
        return tables_df
    
    
    def load_data_to_bigquery(self, tables_df):

        for dataset in self.datasets:
            self.bigquery.create_dataset(dataset)

        for key in tables_df.keys():
            self.bigquery.load_table(tables_df[key], 'raw', key.lower())
        return

    def dbt(self):

        result = subprocess.run(
            ['dbt', 'run'],
            cwd=os.path.join(os.path.dirname(__file__), 'transform', 'shopify'),  
            capture_output=True,
            text=True,
            env={**os.environ, 'DBT_PROFILES_DIR': self.config_path}  
        )
        
        if result.returncode == 0:
            print("DBT model build successful")
        else:
            print(f"DBT model build failed: {result.stdout}")
    

    def run(self):

        tables_df = self.fetch_data()
        self.load_data_to_bigquery(tables_df)
        self.dbt()

        return 'Shopify ETL Process Completed', 200


# Google Cloud Function Entry Point
def main(request):
    
    config_path = os.path.join(os.path.dirname(__file__), 'config')
    pipeline = Pipeline(config_path, local_deployment=False)    
    
    return pipeline.run()



if __name__ == '__main__':
    
    config_path = os.path.join(os.path.dirname(__file__), 'config')
    pipeline = Pipeline(config_path)
    pipeline.run()
     