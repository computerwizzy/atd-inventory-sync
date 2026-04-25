"""Fix the 11 remaining Method products with no image.
Look up each by SKU in Shopify, fetch 3 angles from AutoSync (falling back to
model+color query), and upload.
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

AS_KEY = os.environ["AUTOSYNC_API_KEY"]
SHOP_TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
SHOP_STORE = os.environ["SHOPIFY_STORE_URL"]
SHOP_H = {"X-Shopify-Access-Token": SHOP_TOKEN, "Content-Type": "application/json"}
IMG_BASE = "https://wheels.autosyncstudio.com/png/"

SKUS = [
    "MR31878550900", "MR31978562900", "MR70478555800", "MR70366568890",
    "MR30421060318N", "MR30421016318N", "MR30489060312N", "MR30478516300",
    "MR30468060300", "MR50278012138-2", "MR50257051115SC",
]

sess = requests.Session()

# Load import csv to get model/color/handle for each SKU
imp = pd.read_csv(HERE / "method_shopify_import.csv", dtype=str).fillna("")
MODEL_COL = "Wheel Model (product.metafields.custom.wheel_model)"
COLOR_COL = "Color (product.metafields.custom.color)"
def norm_color(c): return (c or "").strip().strip("[]").strip('"').strip()
imp["_model"] = imp[MODEL_COL].str.upper().str.strip()
imp["_color"] = imp[COLOR_COL].map(norm_color)
sku_to_meta = {r["Variant SKU"]: (r["Handle"], r["_model"], r["_color"]) for _, r in imp.iterrows()}

def swap_ext(p, ext=".png"):
    return os.path.splitext(p)[0] + ext

def wheel_angles(w):
    out = []
    for a in ("Img0001", "Img0002", "Img0003"):
        v = (w.get(a) or "").strip()
        if v: out.append((a, IMG_BASE + swap_ext(v)))
    return out

def autosync_lookup(sku, model, color):
    # Try PN variations
    pns = [sku, sku.replace("-", ""), sku.rstrip("-2"), sku[:-2] if sku.endswith("SC") else sku]
    for pn in set(pns):
        r = sess.get("https://api.autosyncstudio.com/wheels",
                     params={"key": AS_KEY, "f-pn": pn,
                             "i-img0001":"true","i-img0002":"true","i-img0003":"true"}, timeout=30)
        ws = r.json().get("Wheels", [])
        if ws:
            a = wheel_angles(ws[0])
            if a: return a, f"pn={pn}"
    # Try model+color full-text
    num = model.replace("MR","").lstrip("0")
    for q in ([f"MR{num} {color}"] if color else []) + [f"MR{num}"]:
        r = sess.get("https://api.autosyncstudio.com/wheels",
                     params={"key": AS_KEY, "f-query": q, "f-brand": "Method",
                             "i-img0001":"true","i-img0002":"true","i-img0003":"true"}, timeout=30)
        ws = r.json().get("Wheels", [])
        for w in ws:
            a = wheel_angles(w)
            if a: return a, f"query={q} pn={w.get('Pn')}"
    return [], ""

def get_product_by_handle(handle):
    r = sess.get(f"https://{SHOP_STORE}/admin/api/2024-10/products.json",
                 headers=SHOP_H, params={"handle": handle, "fields": "id,handle,images"}, timeout=30)
    ps = r.json().get("products", [])
    return ps[0] if ps else None

def upload(pid, url, position):
    r = sess.post(f"https://{SHOP_STORE}/admin/api/2024-10/products/{pid}/images.json",
                  headers=SHOP_H, json={"image":{"src":url,"position":position}}, timeout=60)
    if r.status_code == 429:
        time.sleep(3)
        r = sess.post(f"https://{SHOP_STORE}/admin/api/2024-10/products/{pid}/images.json",
                      headers=SHOP_H, json={"image":{"src":url,"position":position}}, timeout=60)
    return r

log = []
for sku in SKUS:
    meta = sku_to_meta.get(sku)
    if not meta:
        log.append({"sku":sku,"status":"NOT_IN_IMPORT_CSV"}); continue
    handle, model, color = meta
    prod = get_product_by_handle(handle)
    if not prod:
        log.append({"sku":sku,"handle":handle,"status":"NOT_IN_SHOPIFY"}); continue
    pid = prod["id"]
    if prod.get("images"):
        log.append({"sku":sku,"handle":handle,"status":"ALREADY_HAS_IMAGE","count":len(prod['images'])}); continue
    angles, src = autosync_lookup(sku, model, color)
    if not angles:
        log.append({"sku":sku,"handle":handle,"model":model,"color":color,"status":"NO_AUTOSYNC_MATCH"}); continue
    uploaded = 0
    urls = []
    for pos, (ang, url) in enumerate(angles, 1):
        r = upload(pid, url, pos)
        if r.status_code in (200, 201):
            uploaded += 1; urls.append(url)
        time.sleep(0.35)
    log.append({"sku":sku,"handle":handle,"model":model,"color":color,
                "status":f"UPLOADED_{uploaded}","source":src,"urls":"|".join(urls)})

for r in log: print(r)

# Save
out = HERE / "fix_remaining_11_log.csv"
keys = ["sku","handle","model","color","status","count","source","urls"]
with out.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
    w.writeheader(); w.writerows(log)
print(f"\n->{out}")
