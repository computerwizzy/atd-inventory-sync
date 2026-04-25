"""Attach recovered images directly to the Shopify products that are missing images."""
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
H = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}

fix = pd.read_csv(HERE / "method_image_fix_FINAL.csv", dtype=str).fillna("")
missing = pd.read_csv(HERE / "products_missing_images_yesterday.csv", dtype=str).fillna("")
handle_to_pid = dict(zip(missing["handle"], missing["product_id"]))

progress_path = HERE / "upload_progress.txt"
log_path = HERE / "upload_log.csv"

uploaded = 0; skipped = 0; failed = 0
with log_path.open("w", newline="", encoding="utf-8") as logf:
    lw = csv.writer(logf)
    lw.writerow(["handle","product_id","status","detail"])
    for i, r in fix.iterrows():
        h = r["Handle"]; img = r["Image Src"].strip(); alt = r["Image Alt Text"]
        pid = handle_to_pid.get(h)
        if not pid or not img:
            lw.writerow([h, pid or "", "SKIP", "no pid or no image"]); skipped += 1; continue
        try:
            resp = requests.post(
                f"https://{STORE}/admin/api/2024-10/products/{pid}/images.json",
                headers=H,
                json={"image": {"src": img, "alt": alt}},
                timeout=60,
            )
            if resp.status_code == 429:
                time.sleep(3); uploaded_this = False
                resp = requests.post(
                    f"https://{STORE}/admin/api/2024-10/products/{pid}/images.json",
                    headers=H, json={"image": {"src": img, "alt": alt}}, timeout=60)
            if resp.status_code in (200, 201):
                lw.writerow([h, pid, "OK", resp.json().get("image",{}).get("src","")]); uploaded += 1
            else:
                lw.writerow([h, pid, f"FAIL_{resp.status_code}", resp.text[:200]]); failed += 1
        except Exception as e:
            lw.writerow([h, pid, "EXC", str(e)[:200]]); failed += 1
        logf.flush()
        if i % 10 == 0:
            progress_path.write_text(
                f"{i+1}/{len(fix)} uploaded={uploaded} skipped={skipped} failed={failed}\n",
                encoding="utf-8")
        time.sleep(0.3)  # Shopify limit is 2 req/s

progress_path.write_text(
    f"DONE {len(fix)} uploaded={uploaded} skipped={skipped} failed={failed}\n",
    encoding="utf-8")
print(f"uploaded={uploaded} skipped={skipped} failed={failed}")
