"""Upload images to Shopify for the 160 missing-image products, downloading bytes
when necessary and posting as base64 attachment (bypasses Shopify URL fetch).

Priority order per handle:
  1. Import CSV Image Src, only if it contains /files/ (working Shopify CDN path)
  2. Master xlsx IMAGE 1/2 LINK (Box URL) for the handle's SKU -> download bytes
  3. Sibling with same (model,color) in import CSV that has a /files/ URL
"""
import base64
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
SHOP_H = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}

imp = pd.read_csv(HERE / "method_shopify_import.csv", dtype=str).fillna("")
missing = pd.read_csv(HERE / "products_missing_images_yesterday.csv", dtype=str).fillna("")

MODEL_COL = "Wheel Model (product.metafields.custom.wheel_model)"
COLOR_COL = "Color (product.metafields.custom.color)"
def norm_color(c): return (c or "").strip().strip("[]").strip('"').strip().lower()
imp["_model"] = imp[MODEL_COL].str.upper().str.strip()
imp["_color"] = imp[COLOR_COL].map(norm_color)

handle_to_sku = dict(zip(imp["Handle"], imp["Variant SKU"]))
handle_to_meta = dict(zip(imp["Handle"], zip(imp["_model"], imp["_color"])))
handle_to_imp_img = dict(zip(imp["Handle"], imp["Image Src"].str.strip()))
handle_to_pid = dict(zip(missing["handle"], missing["product_id"]))

# Sibling /files/ URL by (model,color)
mc_to_files = {}
for _, r in imp.iterrows():
    s = r["Image Src"].strip()
    if "/files/" in s:
        mc_to_files.setdefault((r["_model"], r["_color"]), (s, r["Image Alt Text"]))

# Master xlsx SKU -> Box URLs
sku_to_box = {}
for sheet, hdr in [("WHEELS", 1), ("DISCONTINUED WHEELS", 2)]:
    try:
        mx = pd.read_excel(HERE / "Custom Wheelhouse Master Part List 3-18-2026.xlsx",
                           sheet_name=sheet, header=hdr, dtype=str).fillna("")
    except Exception: continue
    sku_col = next((c for c in mx.columns if "PART NUMBER" in str(c).upper()), None)
    if not sku_col: continue
    for _, r in mx.iterrows():
        sku = str(r[sku_col]).strip()
        if not sku: continue
        for ic in ["IMAGE 1 LINK", "IMAGE 2 LINK"]:
            if ic in mx.columns:
                v = str(r[ic]).strip()
                if v.startswith("http"):
                    sku_to_box.setdefault(sku, v); break

print(f"master SKU->Box: {len(sku_to_box)}   (model,color)->/files/: {len(mc_to_files)}")

sess = requests.Session()

def fetch_bytes(url):
    """Follow redirects; return (bytes, None) or (None, err). Handle Box share links."""
    try:
        r = sess.get(url, timeout=45, allow_redirects=True)
        if r.status_code == 200 and r.content and len(r.content) > 500:
            ct = r.headers.get("content-type", "").lower()
            if "image" in ct or ct.startswith("application/octet"):
                return r.content, None
            # Box sometimes returns HTML share page; try appending ?download=true
            if "box.com" in url and "?download=true" not in url:
                r2 = sess.get(url + ("&" if "?" in url else "?") + "download=true", timeout=45, allow_redirects=True)
                if r2.status_code == 200 and r2.content:
                    return r2.content, None
            return None, f"not-image ct={ct}"
        return None, f"status={r.status_code}"
    except Exception as e:
        return None, str(e)[:100]

def upload(pid, img_bytes=None, src_url=None, alt=""):
    if img_bytes is not None:
        payload = {"image": {"attachment": base64.b64encode(img_bytes).decode(), "alt": alt}}
    else:
        payload = {"image": {"src": src_url, "alt": alt}}
    r = sess.post(f"https://{STORE}/admin/api/2024-10/products/{pid}/images.json",
                  headers=SHOP_H, json=payload, timeout=60)
    if r.status_code == 429:
        time.sleep(3)
        r = sess.post(f"https://{STORE}/admin/api/2024-10/products/{pid}/images.json",
                      headers=SHOP_H, json=payload, timeout=60)
    return r

log_path = HERE / "upload_log_v3.csv"
progress_path = HERE / "upload_progress.txt"
ok = skip = fail = 0

with log_path.open("w", newline="", encoding="utf-8") as f:
    lw = csv.writer(f); lw.writerow(["handle","product_id","strategy","status","detail"])
    handles = missing["handle"].tolist()
    for i, h in enumerate(handles):
        pid = handle_to_pid.get(h)
        if not pid:
            lw.writerow([h, "", "NONE", "SKIP", "no pid"]); skip += 1; continue

        strategies = []
        # 1) own /files/ URL
        own = handle_to_imp_img.get(h, "")
        if "/files/" in own:
            strategies.append(("own_files", own, False))
        # 2) master Box by SKU (download bytes)
        sku = handle_to_sku.get(h, "")
        for cand in [sku, sku.lstrip("Z"), sku[:-1] if len(sku)>3 else ""]:
            if cand and cand in sku_to_box:
                strategies.append(("master_box", sku_to_box[cand], True)); break
        # 3) sibling /files/
        mc = handle_to_meta.get(h)
        if mc and mc in mc_to_files:
            strategies.append(("sibling_files", mc_to_files[mc][0], False))

        if not strategies:
            lw.writerow([h, pid, "NONE", "SKIP", "no source"]); skip += 1; continue

        done = False
        for name, url, fetch in strategies:
            if fetch:
                b, err = fetch_bytes(url)
                if not b:
                    lw.writerow([h, pid, name, "FETCH_FAIL", f"{url[:80]} {err}"]); continue
                r = upload(pid, img_bytes=b, alt="")
            else:
                r = upload(pid, src_url=url, alt="")
            if r.status_code in (200, 201):
                lw.writerow([h, pid, name, "OK", url[:80]]); ok += 1; done = True; break
            else:
                lw.writerow([h, pid, name, f"FAIL_{r.status_code}", r.text[:120]])
        if not done:
            fail += 1
        f.flush()

        if i % 5 == 0:
            progress_path.write_text(f"{i+1}/{len(handles)} ok={ok} skip={skip} fail={fail}\n", encoding="utf-8")
        time.sleep(0.4)

progress_path.write_text(f"DONE ok={ok} skip={skip} fail={fail}\n", encoding="utf-8")
print(f"DONE ok={ok} skip={skip} fail={fail}")
