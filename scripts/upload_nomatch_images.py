"""For the 33 AutoSync NO_MATCH products, pull images from alternative web sources
and upload to Shopify.

MR319 Bronze (16 products) -> methodracewheels.com/products/319-method-bronze
MR201 Silver (2)           -> Summit s0456 single angle
MR405 Bronze (4) + Black(4)-> Summit s0623/s0631 single angle
MR406 Black  (7)           -> Summit s0408/s0418 single angle
"""
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

SHOP_TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
SHOP_STORE = os.environ["SHOPIFY_STORE_URL"]
SHOP_H = {"X-Shopify-Access-Token": SHOP_TOKEN, "Content-Type": "application/json"}

sess = requests.Session()
sess.headers["User-Agent"] = "Mozilla/5.0"

# ------- Fetch MR319 Method Bronze studio images from Method's own site -------
r = sess.get("https://www.methodracewheels.com/products/319-method-bronze.json", timeout=20)
method_imgs = [im["src"] for im in r.json().get("product", {}).get("images", [])][:3]
print(f"MR319 Bronze images: {len(method_imgs)}")

# ------- Summit placeholders (single angle each) -------
SUMMIT = lambda code: f"https://static.summitracing.com/global/images/prod/xlarge/{code}_xl.jpg"
# Try xlarge first, fallback to _ml
def pick_summit(codes):
    for c in codes:
        for size in ("xl","ml"):
            u = f"https://static.summitracing.com/global/images/prod/{'xlarge' if size=='xl' else 'mediumlarge'}/{c}_{size}.jpg"
            r = sess.head(u, timeout=10)
            if r.status_code == 200:
                return u
    return None

mr405_bronze = pick_summit(["mth-s0631", "mth-s0623", "mth-s0630"])
mr405_black  = pick_summit(["mth-s0619", "mth-s0620", "mth-s0621", "mth-s0622"])
mr406_black  = pick_summit(["mth-s0418", "mth-s0408", "mth-s0419", "mth-s0420"])
mr201_silver = pick_summit(["mth-s0456", "mth-s0457", "mth-s0458"])
print(f"MR405 bronze: {mr405_bronze}")
print(f"MR405 black:  {mr405_black}")
print(f"MR406 black:  {mr406_black}")
print(f"MR201 silver: {mr201_silver}")

# Build resolver
def resolve(model, color):
    m = (model or "").upper()
    c = (color or "").lower()
    if m == "MR319" and "bronze" in c: return method_imgs
    if m == "MR405" and "bronze" in c and mr405_bronze: return [mr405_bronze]
    if m == "MR405" and "black"  in c and mr405_black:  return [mr405_black]
    if m == "MR406" and "black"  in c and mr406_black:  return [mr406_black]
    if m == "MR201" and ("silver" in c or "machined" in c) and mr201_silver: return [mr201_silver]
    return []

# ------- Load the 33 no-match rows -------
log = pd.read_csv(HERE / "upload_log_autosync.csv", dtype=str).fillna("")
no_match = log[log["status"] == "NO_MATCH"].copy()
no_match[["m","c"]] = no_match["pn_tried"].str.extract(r"model=(\S+) color=(.*)")
print(f"no-match rows: {len(no_match)}")

def upload(pid, url, position):
    r = sess.post(f"https://{SHOP_STORE}/admin/api/2024-10/products/{pid}/images.json",
                  headers=SHOP_H, json={"image":{"src":url,"position":position}}, timeout=60)
    if r.status_code == 429:
        time.sleep(3)
        r = sess.post(f"https://{SHOP_STORE}/admin/api/2024-10/products/{pid}/images.json",
                      headers=SHOP_H, json={"image":{"src":url,"position":position}}, timeout=60)
    return r

out_log = HERE / "upload_log_nomatch.csv"
ok = fail = skip = 0
with out_log.open("w", newline="", encoding="utf-8") as f:
    lw = csv.writer(f); lw.writerow(["handle","product_id","model","color","position","status","url","detail"])
    for _, row in no_match.iterrows():
        h = row["handle"]; pid = row["product_id"]; m = row["m"]; c = row["c"]
        imgs = resolve(m, c)
        if not imgs:
            lw.writerow([h, pid, m, c, 0, "NO_SOURCE", "", ""]); skip += 1; continue
        for pos, url in enumerate(imgs, 1):
            r = upload(pid, url, pos)
            if r.status_code in (200, 201):
                lw.writerow([h, pid, m, c, pos, "OK", url, ""]); ok += 1
            else:
                lw.writerow([h, pid, m, c, pos, f"FAIL_{r.status_code}", url, r.text[:120]]); fail += 1
            time.sleep(0.4)
        f.flush()

print(f"DONE ok={ok} fail={fail} skip={skip}")
