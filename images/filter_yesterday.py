"""For each no-image product, fetch created_at; list all dates + yesterday's."""
import csv
import os
import time
import requests
from collections import Counter
from pathlib import Path

HERE = Path(r"c:/Users/DELL-i7/Downloads/wheel1_not_in _store")

for line in (HERE / ".env").read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1)
        os.environ[k.strip()] = v.strip()

TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
STORE = os.environ["SHOPIFY_STORE_URL"]
H = {"X-Shopify-Access-Token": TOKEN}

rows = list(csv.DictReader((HERE / "products_missing_images.csv").open(encoding="utf-8")))
print(f"Checking {len(rows)} no-image products...")

date_counts = Counter()
all_rows = []

for i, r in enumerate(rows):
    pid = r["product_id"]
    try:
        resp = requests.get(
            f"https://{STORE}/admin/api/2024-10/products/{pid}.json",
            params={"fields": "id,handle,title,status,created_at"},
            headers=H, timeout=30,
        )
        if resp.status_code == 429:
            time.sleep(2); continue
        p = resp.json().get("product") or {}
        ca = p.get("created_at", "") or ""
        date = ca[:10]
        date_counts[date] += 1
        all_rows.append({
            "handle": r["handle"], "product_id": pid, "status": r["status"],
            "title": p.get("title", r.get("title","")), "created_at": ca
        })
    except Exception as e:
        print(f"err {pid}: {e}")
    time.sleep(0.08)

# Full dump
out_all = HERE / "products_missing_images_with_dates.csv"
with out_all.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["handle","product_id","status","title","created_at"])
    w.writeheader(); w.writerows(all_rows)

# Yesterday = 2026-04-21
YEST = "2026-04-21"
yest = [r for r in all_rows if r["created_at"].startswith(YEST)]
out_y = HERE / "products_missing_images_yesterday.csv"
with out_y.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["handle","product_id","status","title","created_at"])
    w.writeheader(); w.writerows(yest)

print("\nDate breakdown of no-image products:")
for d, n in sorted(date_counts.items()):
    print(f"  {d}: {n}")
print(f"\nYesterday ({YEST}): {len(yest)} no-image products")
