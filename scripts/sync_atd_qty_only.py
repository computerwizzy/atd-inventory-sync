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
    'OEP', 'Pacer', 'Platinum', 'TIS', 'TIS Motorsports', 'Ultra', 'Raceline', 'Raceline Forged'
}

CSV_FIELDS = ['Brand', 'Model', 'Pn', 'MFG', 'Price', 'MAP', 'Inventory']

PRICE_FILE = 'pricefile_for_location_573314.csv'


def clean_val(val):
    if not val:
        return ''
    val = val.strip().replace('$', '').replace(',', '')
    import re
    m = re.match(r'="(.+)"', val)
    return m.group(1) if m else val


def parse_price_file(local_path):
    prices = {}
    if not os.path.exists(local_path):
        return prices
    with open(local_path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            sku = clean_val(row.get(' Supplier No', ''))
            price = clean_val(row.get(' Price', '').replace('$', ''))
            map_p = clean_val(row.get(' MAP', '').replace('$', ''))
            if sku:
                prices[sku] = {
                    'price': f"{float(price):.2f}" if price else '0.00',
                    'map':   f"{float(map_p):.2f}" if map_p else '0.00',
                }
    return prices


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


def set_inventory_rest(inventory_item_id, location_id, available):
    """Directly set available quantity via REST API — more reliable than GraphQL on-hand mutation."""
    item_num = str(inventory_item_id).split('/')[-1]
    loc_num = str(location_id).split('/')[-1]
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/2024-01/inventory_levels/set.json"
    headers = {'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN, 'Content-Type': 'application/json'}
    while True:
        try:
            resp = requests.post(url, headers=headers, json={
                'location_id': int(loc_num),
                'inventory_item_id': int(item_num),
                'available': available
            })
            if resp.status_code == 429:
                time.sleep(5)
                continue
            if resp.status_code not in (200, 201):
                logging.error(f"REST set inventory failed {resp.status_code}: {resp.text[:200]}")
            return resp.json()
        except Exception as e:
            logging.error(f"REST inventory error: {e}")
            time.sleep(2)
            return None


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

    inv_local   = os.path.join(FEED_DIR, 'latest_atd_inventory.csv')
    price_local = os.path.join(FEED_DIR, 'pricefile_for_location_573314.csv')

    if not download_ftp_file(inv_remote, inv_local):
        return
    download_ftp_file(PRICE_FILE, price_local)
    price_map = parse_price_file(price_local)
    logging.info(f"Price file: {len(price_map)} SKUs loaded")

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
        p = price_map.get(sku, {})
        entry = {
            'Brand':     brand,
            'Model':     row.get('ProductDescription', '').strip(),
            'Pn':        sku,
            'MFG':       row.get('ManufacturerPartNumber', '').strip(),
            'Price':     p.get('price', '0.00'),
            'MAP':       p.get('map', '0.00'),
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

    # Step 1: Update quantities for SKUs currently in ATD feed
    atd_update_count = 0
    for sku, node in shopify_data.items():
        if sku in inventory_qty:
            set_inventory_rest(node['inventoryItem']['id'], SHOPIFY_LOCATION_ID, inventory_qty[sku])
            atd_update_count += 1
    logging.info(f"Updated {atd_update_count} ATD SKUs")

    # Step 2: Zero stale items — qty > 0 at ATD location but no longer in ATD feed
    logging.info(f"Checking ATD location {SHOPIFY_LOCATION_ID} for stale quantities...")
    stale_count = 0
    has_next_page = True
    cursor = None
    while has_next_page:
        query = """
        query getATDLevels($location: ID!, $cursor: String) {
          location(id: $location) {
            inventoryLevels(first: 250, after: $cursor) {
              pageInfo { hasNextPage endCursor }
              edges {
                node {
                  available
                  item { id sku }
                }
              }
            }
          }
        }
        """
        res = shopify_graphql_request(query, {'location': SHOPIFY_LOCATION_ID, 'cursor': cursor})
        if not res or 'data' not in res:
            logging.error("Failed to fetch ATD location inventory levels")
            break
        location_data = res['data'].get('location') or {}
        if not location_data:
            logging.error(f"Location {SHOPIFY_LOCATION_ID} not found — check SHOPIFY_LOCATION_ID secret")
            break
        levels = location_data.get('inventoryLevels', {})
        for edge in levels.get('edges', []):
            node = edge['node']
            qty = node.get('available', 0) or 0
            sku = node['item'].get('sku', '')
            item_id = node['item'].get('id')
            if qty > 0 and sku and sku not in inventory_qty:
                stale_count += 1
                logging.info(f"Stale SKU {sku}: available={qty}, zeroing at ATD location")
                set_inventory_rest(item_id, SHOPIFY_LOCATION_ID, 0)
        page_info = levels.get('pageInfo', {})
        has_next_page = page_info.get('hasNextPage', False)
        cursor = page_info.get('endCursor')

    logging.info(f"Zeroed {stale_count} stale SKUs at ATD location")

    # Upload both CSVs to WBR FTP
    upload_to_wbr(tires_file, 'ATD_Tires_Inventory.csv')
    upload_to_wbr(wheels_file, 'ATD_Wheels_Inventory.csv')

    logging.info(f"Sync complete in {datetime.datetime.now() - start_time}")


if __name__ == "__main__":
    main()