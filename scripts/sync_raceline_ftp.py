import os
import csv
import io
import ftplib
import logging
import datetime
import requests
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
FEED_DIR = os.path.join(BASE_DIR, 'feeds')
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(FEED_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f'raceline_sync_{datetime.date.today()}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)

load_dotenv(os.path.join(BASE_DIR, '.env'))

INVENTORY_URL = 'https://alliedwheel.tireweb.com/treadsearch/inventory.asp?key=H92nj53w3Ue&user=TEP02&passhash=03b9222f54b31305fff140b792678365'

WBR_FTP_HOST = os.environ.get('FTP_HOST')
WBR_FTP_USER = os.environ.get('FTP_USER')
WBR_FTP_PASS = os.environ.get('FTP_PASS')

OUT_FILENAME = 'Raceline_Inventory.csv'
OUT_FILE = os.path.join(FEED_DIR, OUT_FILENAME)
OUT_FIELDS = ['ProductCode', 'Brand', 'Description', 'Stock', 'StockOther', 'TotalStock']


def download_inventory():
    logging.info("Downloading Raceline/Allied inventory feed...")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    resp = requests.get(INVENTORY_URL, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.text


def parse_inventory(raw_csv):
    reader = csv.DictReader(io.StringIO(raw_csv))
    rows = []
    for row in reader:
        pn = row.get('ProductCode', '').strip()
        if not pn:
            continue
        stock = int(float(row.get('Stock', 0) or 0))
        stock_other = int(float(row.get('StockOther', 0) or 0))
        rows.append({
            'ProductCode': pn,
            'Brand':       row.get('Brand', '').strip(),
            'Description': row.get('Description', '').strip(),
            'Stock':       stock,
            'StockOther':  stock_other,
            'TotalStock':  stock + stock_other,
        })
    return rows


def write_csv(rows):
    with open(OUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=OUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    logging.info(f"Wrote {len(rows)} rows to {OUT_FILENAME}")


def upload_to_wbr():
    logging.info(f"Uploading {OUT_FILENAME} to WBR FTP...")
    ftp = ftplib.FTP(WBR_FTP_HOST, timeout=60)
    ftp.login(WBR_FTP_USER, WBR_FTP_PASS)
    with open(OUT_FILE, 'rb') as f:
        ftp.storbinary(f'STOR {OUT_FILENAME}', f)
    ftp.quit()
    logging.info(f"{OUT_FILENAME} uploaded OK")


def main():
    start = datetime.datetime.now()
    logging.info("=== Raceline/Allied Sync Started ===")

    try:
        raw = download_inventory()
    except Exception as e:
        logging.error(f"Failed to download inventory: {e}")
        return

    rows = parse_inventory(raw)
    logging.info(f"Parsed {len(rows)} products")

    if not rows:
        logging.error("No data parsed — aborting")
        return

    write_csv(rows)

    try:
        upload_to_wbr()
    except Exception as e:
        logging.error(f"WBR FTP upload error: {e}")

    logging.info(f"=== Raceline/Allied Sync Done in {datetime.datetime.now() - start} ===")


if __name__ == "__main__":
    main()
