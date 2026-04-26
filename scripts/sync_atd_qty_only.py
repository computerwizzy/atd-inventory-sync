import os
import ftplib
import csv
import logging
import requests
from dotenv import load_dotenv
from collections import defaultdict
import datetime
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
FEED_DIR = os.path.join(BASE_DIR, 'feeds')
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(FEED_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f'atd_qty_sync_{datetime.date.today()}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)

load_dotenv(os.path.join(BASE_DIR, '.env'))

# ATD FTP (source)
FTP_HOST = os.environ.get('ATD_FTP_HOST')
FTP_USER = os.environ.get('ATD_FTP_USER')
FTP_PASS = os.environ.get('ATD_FTP_PASS')
FTP_DIR  = os.environ.get('ATD_FTP_DIR', '/uploads/wheels_below_retail/')

# WBR FTP (destination)
WBR_FTP_HOST = os.environ.get('FTP_HOST')
WBR_FTP_USER = os.environ.get('FTP_USER')
WBR_FTP_PASS = os.environ.get('FTP_PASS')

# Shopify
SHOPIFY_STORE_URL    = os.environ.get('SHOPIFY_STORE_URL')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_LOCATION_ID  = os.environ.get('SHOPIFY_LOCATION_ID', 'gid://shopify/Location/91638464747')

WHEEL_BRANDS = {
    'Advanti Racing', 'Boyd Coddington', 'Cragar', 'Dropstars', 'Dropstars Trail Series',
    'Edge Off Road', 'Edge Street', 'Fittipaldi', 'Focal', 'Gear Off Road', 'Konig',
    'Mamba', 'Maxxim', 'Mickey Thompson', 'Motiv', 'Motiv Offroad', 'OE Performance',
    'OEP', 'Pacer', 'Platinum', 'TIS', 'TIS Motorsports', 'Ultra'
}

CSV_FIELDS = ['Brand', 'Model', 'Pn', 'MFG', 'Price', 'Retail', 'Inventory']


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
                else:
                    return data
            else:
                cost = data.get('extensions', {}).get('cost', {})
                if cost.get('throttleStatus', {}).get('currentlyAvailable', 1000) < 500:
                    time.sleep(2)
                return data
        except Exception as e:
            logging.error(f"Request error: {e}")
            time.sleep(2)


def upload_to_wbr(local_file, remote_name):
    try:
        logging.info(f"Uploading {remote_name} to {WBR_FTP_HOST}...")
        ftp = ftplib.FTP(WBR_FTP_HOST, timeout=60)
        ftp.login(WBR_FTP_USER, WBR_FTP_PASS)
        with open(local_file, 'rb') as f:
            ftp.storbinary(f'STOR {remote_name}', f)
        ftp.quit()
        logging.info(f"{remote_name} uploaded to ftp.wheelsbelowretail.com")
    except Exception as e:
        logging.error(f"WBR FTP upload error: {e}")


def main():
    start_time = datetime.datetime.now()
    logging.info("Starting ATD Inventory Sync")

    inv_remote = get_latest_inventory_filename()
    if not inv_remote:
        logging.error("Could not find latest inventory file on FTP.")
        return

    inv_local = os.path.join(FEED_DIR, 'latest_atd_inventory.csv')
    if not download_ftp_file(inv_remote, inv_local):
        return

    # Parse inventory and split into tires/wheels
    inventory_qty = defaultdict(int)
    tires_rows = []
    wheels_rows = []

    with open(inv_local, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|')
        brand_qty = defaultdict(int)
        brand_rows = defaultdict(list)
        for row in reader:
            sku = row.get('ManufacturerPartNumber', '').strip()
            brand = row.get('BrandName', '').strip()
            qty = int(row.get('QuantityAvailable', '0') or '0')
            if sku:
                inventory_qty[sku] += qty
                brand_qty[(brand, sku)] += qty
                brand_rows[(brand, sku)] = row

    seen = set()
    for (brand, sku), row in brand_rows.items():
        if sku in seen:
            continue
        seen.add(sku)
        total_qty = inventory_qty[sku]
        entry = {
            'Brand':     brand,
            'Model':     row.get('ProductDescription', '').strip(),
            'Pn':        sku,
            'MFG':       row.get('ManufacturerPartNumber', '').strip(),
            'Price':     '0.00',
            'Retail':    '0',
            'Inventory': total_qty
        }
        if brand in WHEEL_BRANDS:
            wheels_rows.append(entry)
        else:
            tires_rows.append(entry)

    logging.info(f"Tires: {len(tires_rows)} | Wheels: {len(wheels_rows)}")

    # Write CSVs
    tires_file  = os.path.join(FEED_DIR, 'ATD_Tires_Inventory.csv')
    wheels_file = os.path.join(FEED_DIR, 'ATD_Wheels_Inventory.csv')

    for path, rows in [(tires_file, tires_rows), (wheels_file, wheels_rows)]:
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)

    # Sync inventory to Shopify
    logging.info("Fetching Shopify data...")
    shopify_data = {}
    has_next_page = True
    cursor = None
    while has_next_page:
        query = """
        query getVariants($cursor: String) {
          productVariants(first: 250, after: $cursor) {
            pageInfo { hasNextPage endCursor }
            edges { node { id sku inventoryItem { id } } }
          }
        }
        """
        res = shopify_graphql_request(query, {'cursor': cursor})
        if not res or 'data' not in res:
            break
        variants = res.get('data', {}).get('productVariants', {}).get('edges', [])
        for edge in variants:
            node = edge['node']
            if node['sku']:
                shopify_data[node['sku']] = node
        page_info = res.get('data', {}).get('productVariants', {}).get('pageInfo', {})
        has_next_page = page_info.get('hasNextPage', False)
        cursor = page_info.get('endCursor')

    items_to_set_qty = []
    for sku, node in shopify_data.items():
        if sku in inventory_qty:
            items_to_set_qty.append({
                'inventoryItemId': node['inventoryItem']['id'],
                'locationId': SHOPIFY_LOCATION_ID,
                'quantity': inventory_qty[sku]
            })

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
    else:
        logging.info("No matching SKUs found to update.")

    # Upload both CSVs to WBR FTP
    upload_to_wbr(tires_file, 'ATD_Tires_Inventory.csv')
    upload_to_wbr(wheels_file, 'ATD_Wheels_Inventory.csv')

    logging.info(f"Sync complete in {datetime.datetime.now() - start_time}")


if __name__ == "__main__":
    main()