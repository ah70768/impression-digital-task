import os
import shopify
from dotenv import load_dotenv
import pandas as pd

class ShopifyAPI:

    def __init__(self, shop, api_key, api_version):
        self.merchant= shop
        self.token = api_key
        self.api_version = api_version
    
    def create_session(self):
        """Creates a session with the Shopify store."""

        session = shopify.Session(self.merchant, self.api_version, self.token)
        
        shopify.ShopifyResource.activate_session(session)
        shop_info = shopify.Shop.current()
        
        if shop_info is None:
            raise Exception("Session failed failed for {shop_name}".format(shop_name = self.merchant))
        print('Session created succesfully for {shop_name}'.format(shop_name = self.merchant))
    

    def fetch_data(self, object_name):
        """Fetches data given an object name ('Product', 'Order', 'Customer', etc)"""

        all_rows = []
        attribute = getattr(shopify, object_name)
        data = attribute.find(since_id=0, limit=250)

        # handle pagination for larger datasets as limit for Shopify API is 250
        for d in data:
            all_rows.append(d.attributes) # attributes retrieves a dict which represents a row of the table
        while data.has_next_page():
            data = data.next_page()
            for d in data:
                all_rows.append(d.attributes)

        return all_rows
    
    def fetch_tables(self, all_tables):
        """Fetches data from all tables and returns a dictionary of DataFrames"""
        
        all_df = {}

        for t in all_tables:
            df = pd.DataFrame(self.fetch_data(t))
            all_df[t] = df
        
        return all_df

    def save_data(self,tables_df,directory='02_data'):
        """Saves the fetched data into CSV files in a given directory"""

        for key in tables_df.keys():
            file_name = '{t}.csv'.format(t=key)
            output_path = os.path.join(os.path.abspath(os.path.join(os.getcwd(), '../../'+directory)),file_name)
            tables_df[key].to_csv(output_path, index=False)
    
    @staticmethod
    def driver():
        """Driver method to execute the above methods in order"""

        dotenv_path = os.path.join(os.path.dirname(__file__), "..", "config", ".env")
        load_dotenv(dotenv_path)

        shop = os.getenv('SHOP_URL')
        api_key = os.getenv('SHOPIFY_ADMIN_API_ACCESS_TOKEN')
        api_version = os.getenv('SHOPIFY_API_VERSION')

        client = ShopifyAPI(shop, api_key, api_version)
        client.create_session()
        
        table_names = ['Order', 'Product', 'Customer']
        tables_df = client.fetch_tables(table_names)

        client.save_data(tables_df)



if __name__ == "__main__":
     
    # ShopifyAPI.driver()
    pass



