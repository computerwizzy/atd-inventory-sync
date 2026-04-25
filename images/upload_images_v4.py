"""Upload up to 3 images per product (front, side, and a sibling angle if available)."""
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

mc_to_files = {}
for _, r in imp.iterrows():
    s = r["Image Src"].strip()
    if "/files/" in s:
        mc_to_files.setdefault((r["_model"], r["_color"]), s)

# Master xlsx SKU -> [url1, url2]
sku_to_box_list = {}
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
        urls = []
        for ic in ["IMAGE 1 LINK", "IMAGE 2 LINK"]:
            if ic in mx.columns:
                v = str(r[ic]).strip()
                if v.startswith("http"): urls.append(v)
        if urls and sku not in sku_to_box_list:
            sku_to_box_list[sku] = urls

print(f"master SKU->Box list: {len(sku_to_box_list)}   (model,color)->/files/: {len(mc_to_files)}")

sess = requests.Session()

def fetch_bytes(url):
    try:
        r = sess.get(url, timeout=45, allow_redirects=True)
        if r.status_code == 200 and r.content and len(r.content) > 500:
            ct = r.headers.get("content-type", "").lower()
            if "image" in ct or ct.startswith("application/octet"):
                return r.content, None
        # Retry with ?download=true for Box
        if "box.com" in url:
            sep = "&" if "?" in url else "?"
            r2 = sess.get(url + sep + "download=true", timeout=45, allow_redirects=True)
            if r2.status_code == 200 and r2.content and len(r2.content) > 500:
                ct = r2.headers.get("content-type", "").lower()
                if "image" in ct or ct.startswith("application/octet"):
                    return r2.content, None
        return None, f"status={r.status_code}"
    except Exception as e:
        return None, str(e)[:80]

def post_image(pid, img_bytes=None, src_url=None, position=None):
    if img_bytes is not None:
        payload = {"image": {"attachment": base64.b64encode(img_bytes).decode()}}
    else:
        payload = {"image": {"src": src_url}}
    if position: payload["image"]["position"] = position
    r = sess.post(f"https://{STORE}/admin/api/2024-10/products/{pid}/images.json",
                  headers=SHOP_H, json=payload, timeout=60)
    if r.status_code == 429:
        time.sleep(3)
        r = sess.post(f"https://{STORE}/admin/api/2024-10/products/{pid}/images.json",
                      headers=SHOP_H, json=payload, timeout=60)
    return r

log_path = HERE / "upload_log_v4.csv"
progress_path = HERE / "upload_progress.txt"

total_ok = total_fail = 0
handles = missing["handle"].tolist()

with log_path.open("w", newline="", encoding="utf-8") as f:
    lw = csv.writer(f); lw.writerow(["handle","product_id","position","strategy","status","detail"])
    for i, h in enumerate(handles):
        pid = handle_to_pid.get(h)
        if not pid:
            lw.writerow([h, "", 0, "NONE", "SKIP", "no pid"]); continue

        # Collect candidate URLs (ordered, deduped)
        candidates = []  # list of (strategy, url, needs_fetch)

        # 1-2) master xlsx Box URLs (front + side) by SKU
        sku = handle_to_sku.get(h, "")
        for cand in [sku, sku.lstrip("Z") if sku.startswith("Z") else sku, sku[:-1] if len(sku)>3 else ""]:
            if cand and cand in sku_to_box_list:
                for u in sku_to_box_list[cand]:
                    candidates.append(("master_box", u, True))
                break

        # 3) sibling /files/ URL for same model+color (different angle)
        mc = handle_to_meta.get(h)
        if mc and mc in mc_to_files:
            candidates.append(("sibling_files", mc_to_files[mc], False))

        # Fallback: own Image Src if /files/
        own = handle_to_imp_img.get(h, "")
        if "/files/" in own:
            candidates.insert(0, ("own_files", own, False))

        # Dedupe by URL
        seen = set(); unique = []
        for c in candidates:
            if c[1] not in seen:
                seen.add(c[1]); unique.append(c)

        if not unique:
            lw.writerow([h, pid, 0, "NONE", "SKIP", "no sources"]); continue

        pos = 0
        for name, url, fetch in unique[:3]:  # cap at 3
            pos += 1
            if fetch:
                b, err = fetch_bytes(url)
                if not b:
                    lw.writerow([h, pid, pos, name, "FETCH_FAIL", f"{url[:60]} {err}"]); total_fail += 1; continue
                r = post_image(pid, img_bytes=b, position=pos)
            else:
                r = post_image(pid, src_url=url, position=pos)
            if r.status_code in (200, 201):
                lw.writerow([h, pid, pos, name, "OK", url[:80]]); total_ok += 1
            else:
                lw.writerow([h, pid, pos, name, f"FAIL_{r.status_code}", r.text[:120]]); total_fail += 1
            time.sleep(0.35)
        f.flush()
        if i % 5 == 0:
            progress_path.write_text(f"{i+1}/{len(handles)} images_ok={total_ok} images_fail={total_fail}\n", encoding="utf-8")

progress_path.write_text(f"DONE images_ok={total_ok} images_fail={total_fail}\n", encoding="utf-8")
print(f"DONE images_ok={total_ok} images_fail={total_fail}")
