import csv
import os
import ftplib
import io
from collections import defaultdict
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, '.env'))

SRC_HOST  = os.environ.get('USAUTOFORCE_FTP_HOST')
SRC_USER  = os.environ.get('USAUTOFORCE_FTP_USER')
SRC_PASS  = os.environ.get('USAUTOFORCE_FTP_PASS')
SRC_DIR   = '/uploads/usautoforce'
SRC_FILE  = '1290215.csv'

DST_HOST  = os.environ.get('FTP_HOST')
DST_USER  = os.environ.get('FTP_USER')
DST_PASS  = os.environ.get('FTP_PASS')
DST_FILE  = 'US_AutoForce_Inventory.csv'

OUT_FILE  = os.path.join(BASE_DIR, 'scripts', 'scratch', DST_FILE)


def download_usaf_csv():
    print(f"Connecting to {SRC_HOST} as {SRC_USER}...", flush=True)
    buf = io.BytesIO()
    ftp = ftplib.FTP(SRC_HOST, timeout=120)
    ftp.login(SRC_USER, SRC_PASS)
    ftp.cwd(SRC_DIR)
    size = ftp.size(SRC_FILE)
    print(f"Downloading {SRC_FILE} ({size/1024/1024:.1f} MB)...", flush=True)
    ftp.retrbinary(f'RETR {SRC_FILE}', buf.write)
    ftp.quit()
    buf.seek(0)
    print("Download complete.", flush=True)
    return buf


def process(buf):
    text = buf.read().decode('utf-8', errors='replace')
    reader = csv.DictReader(io.StringIO(text))

    # Aggregate inventory per PartNumber across all warehouses
    # Keep first-seen values for Brand/Pattern/TireSize/Cost/RetailPrice/Map
    agg = defaultdict(lambda: {
        'BrandCode': '', 'Pattern': '', 'TireSize': '',
        'Cost': '', 'RetailPrice': '', 'Map': '',
        'Inventory': 0
    })

    row_count = 0
    for row in reader:
        row_count += 1
        pn = row.get('PartNumber', '').strip()
        if not pn:
            continue
        qty = int(float(row.get('QuantityAvailable', 0) or 0))
        entry = agg[pn]
        if not entry['BrandCode']:
            entry['BrandCode']   = row.get('BrandCode', '').strip()
            entry['Pattern']     = row.get('Pattern', '').strip()
            entry['TireSize']    = row.get('TireSize', '').strip()
            entry['Cost']        = row.get('Cost', '').strip()
            entry['RetailPrice'] = row.get('RetailPrice', '').strip()
            entry['Map']         = row.get('Map', '').strip()
        entry['Inventory'] += qty

    print(f"Processed {row_count:,} rows -> {len(agg):,} unique part numbers.", flush=True)
    return agg


def write_csv(agg):
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    fields = ['BrandCode', 'PartNumber', 'Pattern', 'TireSize', 'Cost', 'RetailPrice', 'Map', 'Inventory']
    with open(OUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for pn, v in sorted(agg.items()):
            writer.writerow({
                'BrandCode':   v['BrandCode'],
                'PartNumber':  pn,
                'Pattern':     v['Pattern'],
                'TireSize':    v['TireSize'],
                'Cost':        v['Cost'],
                'RetailPrice': v['RetailPrice'],
                'Map':         v['Map'],
                'Inventory':   v['Inventory'],
            })
    print(f"Saved {len(agg):,} rows to {OUT_FILE}", flush=True)


def upload_csv():
    print(f"Uploading {DST_FILE} to {DST_HOST}...", flush=True)
    ftp = ftplib.FTP(DST_HOST, timeout=60)
    ftp.login(DST_USER, DST_PASS)
    with open(OUT_FILE, 'rb') as f:
        ftp.storbinary(f'STOR {DST_FILE}', f)
    ftp.quit()
    print(f"Done: {DST_FILE} uploaded to {DST_HOST}", flush=True)


def main():
    if not all([SRC_HOST, SRC_USER, SRC_PASS, DST_HOST, DST_USER, DST_PASS]):
        print("ERROR: Missing FTP credentials in .env")
        return

    print("=== US Auto Force FTP -> FTP Sync ===", flush=True)
    buf = download_usaf_csv()
    agg = process(buf)

    if not agg:
        print("No data found. Aborting.")
        return

    write_csv(agg)
    upload_csv()
    print("All done.", flush=True)


if __name__ == "__main__":
    main()
