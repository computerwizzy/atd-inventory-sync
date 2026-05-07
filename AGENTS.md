# ATD Inventory Sync — AI Context

## What this repo does
Downloads the ATD (American Tire Distributors) inventory file from ATD's FTP, parses it into tires and wheels CSVs, syncs quantities to Shopify, and uploads the CSVs to the WheelsbelowRetail FTP for downstream use.

## Data flow
```
ATD FTP (ftp.autosyncstudio.com) → parse → Shopify inventory update
                                          → WBR FTP (ftp.wheelsbelowretail.com)
```

## Schedule
GitHub Actions runs every 4 hours via `.github/workflows/atd_sync.yml`.

## Key script
`scripts/sync_atd_qty_only.py`

## ATD FTP details
- Host: `ATD_FTP_HOST` (ftp.autosyncstudio.com)
- Dir: `ATD_FTP_DIR` (/uploads/wheels_below_retail/)
- Inventory file: `384408-665056-T1-inventory-*.csv` (pipe-delimited, picks latest)
- Price file: `pricefile_for_location_573314.csv`

## Shopify details
- Store: `rines-and-wheels.myshopify.com`
- Location: `gid://shopify/Location/91638464747` (ATD Warehouse)
- Uses `inventorySetOnHandQuantities` mutation (NOT `inventoryActivate`)

## Output files on WBR FTP
- `ATD_Tires_Inventory.csv`
- `ATD_Wheels_Inventory.csv`

## Environment variables required
```
ATD_FTP_HOST, ATD_FTP_USER, ATD_FTP_PASS, ATD_FTP_DIR
FTP_HOST, FTP_USER, FTP_PASS          # WBR FTP destination
SHOPIFY_STORE_URL, SHOPIFY_ACCESS_TOKEN, SHOPIFY_LOCATION_ID
```

## Related repos in this ecosystem
| Repo | Role |
|---|---|
| `autosync-ftp-sync` | USAF inventory → Shopify (location `91693121771`) + WBR FTP |
| `raceline-inventory-sync` | Allied/Raceline wheels → WBR FTP (every 6 hrs) |
| `vct-wheels-inventory` | VCT wheels → WBR FTP (scrapes vctwheels.com every 6 hrs) |
| `ATD-Wheels-Inventory` | Reads ATD_Wheels_Inventory.csv from WBR FTP → Shopify |
| `autosync-tires-ftp` | AutoSync API tires → WBR FTP only |
| `autosync-wheels-ftp` | AutoSync API wheels → WBR FTP only |
| `wheelpro-invenroty-feed` | WheelPros SFTP → WBR FTP only |
| `ebay-inventory-sync` | Shopify → eBay (reverse direction) |

## GitHub Actions workflow versions
All workflows must use `actions/checkout@v4` and `actions/setup-python@v5`.
Do NOT use v5/v6 — they don't exist and will silently fail before running the script.