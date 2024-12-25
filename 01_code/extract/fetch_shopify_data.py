import os
import shopify
from dotenv import load_dotenv
import pandas as pd

class ShopifyAPI:

    def __init__(self, shop, api_key, api_version):
        """
        Initializes the ShopifyAPI instance with the store details and API credentials.

        Args:
            shop (str): The Shopify store URL.
            api_key (str): The API access token for authentication.
            api_version (str): The API version to be used.
        """

        self.merchant= shop
        self.token = api_key
        self.api_version = api_version
    
    def create_session(self):
        """
        Creates and activates a session with the Shopify store.

        This method initializes a Shopify session and verifies it by fetching store details.

        Raises:
            Exception: If the session creation fails.
        """

        session = shopify.Session(self.merchant, self.api_version, self.token)
        
        shopify.ShopifyResource.activate_session(session)
        shop_info = shopify.Shop.current()
        try:

            if shop_info is None:
                print('Failed to create session for {shop_name}'.format(shop_name = self.merchant))
            else:
                print('Session created succesfully for {shop_name}'.format(shop_name = self.merchant))
        except Exception as e:
            print('Error:{error}'.format(error=e))
    

    def fetch_data(self, object_name):
        """
        Fetches data for a specific object type from the Shopify API.

        Args:
            object_name (str): The name of the Shopify object to fetch (e.g., 'Product', 'Order', 'Customer').

        Returns:
            list: A list of dictionaries representing the data fetched.
        """

        all_rows = []
        attribute = getattr(shopify, object_name)
        data = attribute.find(since_id=0, limit=250)

        # handle pagination for larger datasets as limit for Shopify API is 250
        for d in data:
            all_rows.append(d.to_dict()) 
        while data.has_next_page():
            data = data.next_page()
            for d in data:
                all_rows.append(d.to_dict())
        
        print("Data fetched for {attribute}".format(attribute = object_name))

        return all_rows
    
    def fetch_tables(self, all_tables):
        """Fetches data from all tables and returns a dictionary of DataFrames"""
        
        all_df = {}

        for t in all_tables:
            df = pd.DataFrame(self.fetch_data(t))
            all_df[t] = df
        
        return all_df

    def save_data(self,tables_df,directory='02_data'):
        """
        Saves the fetched data into CSV files.

        Args:
            tables_df (dict): A dictionary of pandas DataFrames where keys are table names.
            directory (str): The directory path where the CSV files will be saved. Defaults to '02_data'.
        """
        output_dir = os.path.abspath(os.path.join(os.getcwd(), '../'+directory))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print('Directory {dir} created.'.format(dir=output_dir))
        
        for key in tables_df.keys():
            file_name = '{t}.csv'.format(t=key.lower())
            output_path = os.path.join(output_dir,file_name)
            tables_df[key].to_csv(output_path, index=False)
            print("Saved {k} data to {path}.".format(k=key, path=output_path))
    
    @staticmethod
    def driver():
        """
        Driver method to execute the Shopify API. Use for testiong purposes.

        This method performs the following steps:
        1. Loads environment variables from a `.env` file located in the `config` directory.
        2. Initializes a `ShopifyAPI` client using credentials and configuration from environment variables.
        3. Establishes a session with the Shopify store.
        4. Fetches data for specified Shopify table names (e.g., 'Order', 'Product', 'Customer').
        5. Saves the fetched data as CSV files in the specified directory.
        """

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

    pass



