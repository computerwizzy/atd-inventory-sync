"""Re-check all 160 handles in Shopify to find any that still have no images."""
import csv
import os
import time
import requests
import pandas as pd
from pathlib import Path

HERE = Path(r"c:/Users/DELL-i7/Downloads/wheel1_not_in _store")
for line in (HERE / ".env").read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1); os.environ[k.strip()] = v.strip()

TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
STORE = os.environ["SHOPIFY_STORE_URL"]
H = {"X-Shopify-Access-Token": TOKEN}

missing_df = pd.read_csv(HERE / "products_missing_images_yesterday.csv", dtype=str).fillna("")
sess = requests.Session(); sess.headers.update(H)

still_no_img = []
for i, r in missing_df.iterrows():
    pid = r["product_id"]
    resp = sess.get(f"https://{STORE}/admin/api/2024-10/products/{pid}.json",
                    params={"fields": "id,handle,title,images,status"}, timeout=30)
    if resp.status_code == 429:
        time.sleep(2); continue
    p = resp.json().get("product", {}) or {}
    imgs = p.get("images", [])
    if not imgs:
        still_no_img.append({
            "handle": p.get("handle", r["handle"]),
            "product_id": pid,
            "status": p.get("status", r["status"]),
            "title": p.get("title", r["title"]),
        })
    time.sleep(0.1)

print(f"Still no image: {len(still_no_img)}")
for row in still_no_img:
    print(f"  {row['handle']}  ({row['status']})  {row['title'][:70]}")

if still_no_img:
    out = HERE / "still_no_image.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["handle","product_id","status","title"])
        w.writeheader(); w.writerows(still_no_img)
    print(f"\nSaved -> {out}")
