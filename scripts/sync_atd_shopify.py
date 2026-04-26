import os
import ftplib
import csv
import logging
import requests
from dotenv import load_dotenv
from collections import defaultdict
import datetime
import time
import re

# Configure paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
FEED_DIR = os.path.join(BASE_DIR, 'feeds')
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(FEED_DIR, exist_ok=True)

# Configure logging
log_file = os.path.join(LOG_DIR, f'atd_sync_{datetime.date.today()}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

load_dotenv(os.path.join(BASE_DIR, '.env'))

# --- Configurations ---
# ATD FTP (source)
FTP_HOST = os.environ.get('ATD_FTP_HOST')
FTP_USER = os.environ.get('ATD_FTP_USER')
FTP_PASS = os.environ.get('ATD_FTP_PASS')
FTP_DIR = os.environ.get('ATD_FTP_DIR', '/uploads/wheels_below_retail/')

# WBR FTP (destination)
WBR_FTP_HOST = os.environ.get('FTP_HOST')
WBR_FTP_USER = os.environ.get('FTP_USER')
WBR_FTP_PASS = os.environ.get('FTP_PASS')

# Shopify
SHOPIFY_STORE_URL = os.environ.get('SHOPIFY_STORE_URL')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_LOCATION_ID = os.environ.get('SHOPIFY_LOCATION_ID', 'gid://shopify/Location/91638464747') 

# Files
PRICE_FILE_NAME = os.path.join(FEED_DIR, 'pricefile_for_location_573314.csv')
WP_PRICE_FILE = os.path.join(FEED_DIR, 'tireInvPriceData.csv')

# --- Helper Functions ---

def clean_atd_val(val):
    if not val: return ""
    val = val.strip()
    match = re.match(r'="(.+)"', val)
    if match: val = match.group(1)
    val = val.replace('$', '').replace(',', '')
    return val

def download_ftp_file(remote_name, local_name):
    try:
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)
        with open(local_name, 'wb') as f:
            ftp.retrbinary(f'RETR {remote_name}', f.write)
        ftp.quit()
        return True
    except Exception as e:
        logging.error(f"FTP Error downloading {remote_name}: {e}")
        return False

def get_latest_inventory_filename():
    try:
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)
        files = []
        ftp.retrlines('NLST', files.append)
        inv_files = [f for f in files if f.startswith('384408-665056-T1-inventory-') and f.endswith('.csv')]
        inv_files.sort()
        ftp.quit()
        return inv_files[-1] if inv_files else None
    except Exception as e:
        logging.error(f"FTP Error: {e}")
        return None

def parse_price_list(filename):
    price_data = {}
    if not os.path.exists(filename):
        logging.warning(f"Price file {filename} not found.")
        return {}
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                sku = clean_atd_val(row.get(' Oracle No'))
                map_price = clean_atd_val(row.get(' MAP'))
                brand = clean_atd_val(row.get(' Manufacturer'))
                if sku:
                    price_data[sku] = {
                        'map': float(map_price) if map_price else 0.0,
                        'brand': brand.upper()
                    }
        return price_data
    except Exception as e:
        logging.error(f"Error parsing price list: {e}")
        return {}

def parse_wp_price_list(filename):
    """Parses Wheel Pros tire feed for MAP prices."""
    price_data = {}
    if not os.path.exists(filename):
        logging.warning(f"Wheel Pros file {filename} not found.")
        return {}
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                sku = row.get('ManufacturerPartNumber')
                map_price = row.get('MAP_USD')
                if sku:
                    try:
                        price_data[sku] = float(map_price) if map_price else 0.0
                    except: pass
        return price_data
    except Exception as e:
        logging.error(f"Error parsing Wheel Pros price list: {e}")
        return {}

def shopify_graphql_request(query, variables=None):
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/2024-01/graphql.json"
    headers = {'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN, 'Content-Type': 'application/json'}
    while True:
        try:
            response = requests.post(url, headers=headers, json={'query': query, 'variables': variables})
            data = response.json()
            if 'errors' in data:
                for error in data['errors']:
                    if error.get('extensions', {}).get('code') == 'THROTTLED':
                        time.sleep(5)
                        break
                else: return data
            else:
                cost = data.get('extensions', {}).get('cost', {})
                if cost.get('throttleStatus', {}).get('currentlyAvailable', 1000) < 500: time.sleep(2)
                return data
        except Exception as e:
            logging.error(f"Request error: {e}")
            time.sleep(2)

def update_shopify_prices(items_to_update):
    batch_size = 50
    for i in range(0, len(items_to_update), batch_size):
        batch = items_to_update[i:i+batch_size]
        mutation_lines = []
        for idx, item in enumerate(batch):
            mutation_lines.append(f'  upd{idx}: productVariantUpdate(input: {{ id: "{item["id"]}", price: "{item["new_price"]}" }}) {{ userErrors {{ message }} }}')
        mutation = "mutation {\n" + "\n".join(mutation_lines) + "\n}"
        shopify_graphql_request(mutation)
        logging.info(f"Updated price for batch {i//batch_size + 1}")

def main():
    start_time = datetime.datetime.now()
    logging.info("Starting ATD Inventory & Price Sync (STRICT MODE)")
    
    inv_remote = get_latest_inventory_filename()
    if not inv_remote:
        logging.error("Could not find latest inventory file on FTP.")
        return

    inv_local = os.path.join(FEED_DIR, 'latest_atd_inventory.csv')
    price_local = os.path.join(FEED_DIR, 'atd_price_list.csv')
    
    # Download files
    if not download_ftp_file(inv_remote, inv_local): return
    if not download_ftp_file(os.path.basename(PRICE_FILE_NAME), price_local): return
    
    # Parse Price Maps
    atd_price_map = parse_price_list(price_local)
    wp_price_map = parse_wp_price_list(WP_PRICE_FILE)
    
    # Combine maps (take the highest MAP found between both, preserving brand for validation)
    combined_price_map = {}
    all_skus = set(atd_price_map.keys()) | set(wp_price_map.keys())
    for sku in all_skus:
        atd_data = atd_price_map.get(sku, {'map': 0.0, 'brand': ""})
        wp_p = wp_price_map.get(sku, 0.0)
        
        # Determine highest price and associated brand
        if wp_p > atd_data['map']:
            combined_price_map[sku] = {'map': wp_p, 'brand': ""} # WP brand matching not implemented yet
        else:
            combined_price_map[sku] = atd_data
    
    inventory_qty = defaultdict(int)
    with open(inv_local, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|')
        for row in reader:
            sku = row.get('ManufacturerPartNumber')
            if sku: inventory_qty[sku] += int(row.get('QuantityAvailable', '0'))

    logging.info("Fetching Shopify data...")
    shopify_data = {}
    has_next_page = True
    cursor = None
    while has_next_page:
        query = """
        query getVariants($cursor: String) {
          productVariants(first: 250, after: $cursor) {
            pageInfo { hasNextPage endCursor }
            edges { 
              node { 
                id 
                sku 
                price 
                inventoryItem { id } 
                product { vendor }
              } 
            }
          }
        }
        """
        res = shopify_graphql_request(query, {'cursor': cursor})
        if not res or 'data' not in res: break
        variants = res.get('data', {}).get('productVariants', {}).get('edges', [])
        for edge in variants:
            node = edge['node']
            sku = node['sku']
            if sku:
                shopify_data[sku] = {
                    'id': node['id'],
                    'price': node['price'],
                    'inventoryItemId': node['inventoryItem']['id'],
                    'brand': node['product']['vendor'].upper() if node['product']['vendor'] else ""
                }
        page_info = res.get('data', {}).get('productVariants', {}).get('pageInfo', {})
        has_next_page = page_info.get('hasNextPage', False)
        cursor = page_info.get('endCursor')

    items_to_set_qty = []
    items_to_fix_price = []
    
    for sku, s_node in shopify_data.items():
        # Inventory Sync (Requires SKU Match)
        if sku in inventory_qty:
            items_to_set_qty.append({
                'inventoryItemId': s_node['inventoryItemId'],
                'locationId': SHOPIFY_LOCATION_ID,
                'quantity': inventory_qty[sku]
            })
            
        # Price Sync (STRICT SKU + BRAND MATCH)
        if sku in combined_price_map:
            price_data = combined_price_map[sku]
            map_price = price_data['map']
            a_brand = price_data['brand']
            s_brand = s_node['brand']
            
            # Brand matching: Only enforce if brand is present in feed
            brand_match = True
            if a_brand:
                brand_match = (a_brand in s_brand or s_brand in a_brand)
            
            if brand_match:
                if map_price > 0 and float(s_node['price']) < map_price - 0.01:
                    items_to_fix_price.append({'id': s_node['id'], 'new_price': map_price})
            else:
                logging.warning(f"Brand mismatch for SKU {sku}: Shopify '{s_brand}' vs ATD '{a_brand}'. Skipping price sync.")

    if items_to_set_qty:
        logging.info(f"Syncing inventory for {len(items_to_set_qty)} items...")
        for i in range(0, len(items_to_set_qty), 100):
            batch = items_to_set_qty[i:i+100]
            mutation = """
            mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
              inventorySetOnHandQuantities(input: $input) { userErrors { message } }
            }
            """
            shopify_graphql_request(mutation, {"input": {"reason": "correction", "setQuantities": batch}})
        logging.info("Inventory sync complete.")

    if items_to_fix_price:
        logging.info(f"Fixing {len(items_to_fix_price)} prices below MAP...")
        update_shopify_prices(items_to_fix_price)

    # Upload ATD inventory feed to WBR FTP
    try:
        logging.info(f"Uploading ATD_Inventory.csv to {WBR_FTP_HOST}...")
        ftp = ftplib.FTP(WBR_FTP_HOST, timeout=60)
        ftp.login(WBR_FTP_USER, WBR_FTP_PASS)
        with open(inv_local, 'rb') as f:
            ftp.storbinary('STOR ATD_Inventory.csv', f)
        ftp.quit()
        logging.info("ATD_Inventory.csv uploaded to ftp.wheelsbelowretail.com")
    except Exception as e:
        logging.error(f"WBR FTP upload error: {e}")

    logging.info(f"Sync complete in {datetime.datetime.now() - start_time}")

if __name__ == "__main__":
    main()
