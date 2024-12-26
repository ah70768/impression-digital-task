import os
import subprocess
import yaml
from extract.fetch_shopify_data import ShopifyAPI
from load.bigquery import BigQueryManager


class ETLPipeline:

    def __init__(self, config_path, local_deployment=True):
        """
        Initializes the ETL pipeline.

        Args:
            config_path (str): Path to the configuration directory.
            local_deployment (bool): Flag to indicate local or cloud deployment. Defaults to True.
        """
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
        
        self.data_config = self.load_data_config()

        self.datasets = self.data_config['bigquery']['datasets']
        self.table_names = self.data_config['shopify']['tables']

    
    def load_environment(self):
        """
        Loads environment variables from a YAML configuration file.
        
        Reads the 'env.yml' file from the config path and sets the variables in the environment.
        """

        yaml_path = os.path.join(self.config_path, 'env.yml')      

        with open(yaml_path, 'r') as file:
            config = yaml.safe_load(file) 
        
        for key, value in config.items():
            os.environ[key] = value 
        return

    def load_data_config(self):
        """
        Loads the data configuration from a YAML file.

        This method reads a `data.yml` file located in the `config_path` directory
        and parses its contents using YAML to return the configuration as a dictionary.

        Returns:
            dict: A dictionary containing the data configuration defined in `data.yml`.
        """

        data_config_path = os.path.join(self.config_path, 'data.yml')

        with open(data_config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    
    def fetch_data(self):
        """
        Extracts data from the Shopify API.

        Establishes a session with Shopify and fetches data for the specified table names.

        Returns:
            dict: A dictionary of pandas DataFrames containing the data from the Shopify tables.
        """
        self.shopify_client.create_session()
        tables_df = self.shopify_client.fetch_tables(self.table_names)
        return tables_df
    
    
    def load_data_to_bigquery(self, tables_df):
        """
        Loads data into Google BigQuery.

        Creates datasets and loads data into BigQuery tables.

        Args:
            tables_df (dict): A dictionary of pandas DataFrames to be loaded into BigQuery.
        """

        for dataset in self.datasets:
            self.bigquery.create_dataset(dataset)

        for key in tables_df.keys():
            self.bigquery.load_table(tables_df[key], 'raw', key.lower())
        return

    def run_dbt(self):
        """
        Runs DBT models to transform data.

        Uses the DBT CLI to build models in the 'transform/shopify' directory.
        """

        result = subprocess.run(
            ['dbt', 'build'],
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
        """
        Executes the full ETL pipeline.

        Performs the following steps:
        1. Extracts data from Shopify.
        2. Loads data into BigQuery.
        3. Runs DBT transformations.

        Returns:
            tuple: A message indicating the pipeline status and an HTTP status code.
        """
        tables_df = self.fetch_data()
        self.load_data_to_bigquery(tables_df)
        self.run_dbt()

        return 'ETL Process Completed', 200



def main(request):
    """
    Google Cloud Function Entry Point.

    Initializes and runs the ETL pipeline in a cloud environment. This function is triggered
    when an HTTP request is made to the deployed Google Cloud Function. It performs the
    following steps:
    1. Loads the configuration files from the specified path.
    2. Initializes the ETLPipeline class with the configuration.
    3. Executes the ETL process, including data extraction, loading, and transformation.

    Args:
        request: The HTTP request object triggering the function.

    Returns:
        tuple: A message indicating the pipeline status and an HTTP status code (e.g., 'ETL Process Completed', 200).
    """
    
    config_path = os.path.join(os.path.dirname(__file__), 'config')
    pipeline = ETLPipeline(config_path, local_deployment=False)    
    
    return pipeline.run()



if __name__ == '__main__':
    
    # config_path = os.path.join(os.path.dirname(__file__), 'config')
    # pipeline = Pipeline(config_path)
    # pipeline.run()

    pass
     