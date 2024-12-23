import os
from dotenv import load_dotenv
from extract.fetch_shopify_data import ShopifyAPI
from load.bigquery import BigQueryManager


def main():
    
    dotenv_path = os.path.join(os.path.dirname(__file__), "config", ".env")
    load_dotenv(dotenv_path)

    shop = os.getenv('SHOP_URL')
    api_key = os.getenv('SHOPIFY_ADMIN_API_ACCESS_TOKEN')
    api_version = os.getenv('SHOPIFY_API_VERSION')
    
    client = ShopifyAPI(shop, api_key, api_version)
    client.create_session()
        
    table_names = ['Order', 'Product', 'Customer']
    tables_df = client.fetch_tables(table_names)

    project_id = os.getenv('GCP_PROJECT_ID')
    gcp_creds_path = os.path.join(os.path.dirname(__file__), 'config', 'impression-digital-22a0eecacc9a.json')

    # print(project_id)
    bq = BigQueryManager(project_id, gcp_creds_path)

    # create datasets for each stage of data processing
    datasets = ['raw', 'staging', 'processed']
    for ds in datasets:
        bq.create_dataset(ds)

    for key in tables_df.keys():
        bq.load_table(tables_df[key], 'raw', key.lower())
    


if __name__ == '__main__':
    main()