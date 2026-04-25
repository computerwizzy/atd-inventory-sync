"""Scan already-uploaded Method products for missing images."""
import csv
import os
import time
import requests
import pandas as pd
from pathlib import Path

HERE = Path(r"c:/Users/DELL-i7/Downloads/wheel1_not_in _store")

# Load .env
for line in (HERE / ".env").read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1)
        os.environ[k.strip()] = v.strip()

TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
STORE = os.environ["SHOPIFY_STORE_URL"]
H = {"X-Shopify-Access-Token": TOKEN}

imp = pd.read_csv(HERE / "method_shopify_import.csv", dtype=str).fillna("")
handles = imp["Handle"].drop_duplicates().tolist()

no_img = []
has_img = 0
not_found = 0
progress_path = HERE / "scan_progress.txt"
out_path = HERE / "products_missing_images.csv"

with out_path.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["handle", "product_id", "status", "title"])
    for i, h in enumerate(handles):
        try:
            r = requests.get(
                f"https://{STORE}/admin/api/2024-10/products.json",
                params={"handle": h, "fields": "id,handle,title,images,status"},
                headers=H,
                timeout=30,
            )
            if r.status_code == 429:
                time.sleep(2)
                continue
            products = r.json().get("products", [])
            if not products:
                not_found += 1
            else:
                p = products[0]
                if not p.get("images"):
                    no_img.append((p["handle"], p["id"], p["status"], p.get("title", "")))
                    w.writerow([p["handle"], p["id"], p["status"], p.get("title", "")])
                    f.flush()
                else:
                    has_img += 1
        except Exception as e:
            progress_path.write_text(f"ERROR at {i} {h}: {e}\n", encoding="utf-8")
        if i % 25 == 0:
            progress_path.write_text(
                f"{i}/{len(handles)} has_img={has_img} no_img={len(no_img)} not_found={not_found}\n",
                encoding="utf-8",
            )
        time.sleep(0.1)

progress_path.write_text(
    f"DONE {len(handles)} has_img={has_img} no_img={len(no_img)} not_found={not_found}\n",
    encoding="utf-8",
)
print(f"has_img={has_img} no_img={len(no_img)} not_found={not_found}")
