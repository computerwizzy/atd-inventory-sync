"""Attach images directly to Shopify. Primary source: master xlsx Box URLs.
Skip any /products/ Shopify CDN URLs (they're dead). Retries once with alt URL."""
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

imp = pd.read_csv(HERE / "method_shopify_import.csv", dtype=str).fillna("")
missing = pd.read_csv(HERE / "products_missing_images_yesterday.csv", dtype=str).fillna("")

MODEL_COL = "Wheel Model (product.metafields.custom.wheel_model)"
COLOR_COL = "Color (product.metafields.custom.color)"
def norm_color(c): return (c or "").strip().strip("[]").strip('"').strip().lower()
imp["_model"] = imp[MODEL_COL].str.upper().str.strip()
imp["_color"] = imp[COLOR_COL].map(norm_color)
handle_to_sku   = dict(zip(imp["Handle"], imp["Variant SKU"]))
handle_to_meta  = dict(zip(imp["Handle"], zip(imp["_model"], imp["_color"])))
handle_to_pid   = dict(zip(missing["handle"], missing["product_id"]))

# --- Build SKU -> image URL from master xlsx (Box links) ---
master = HERE / "Custom Wheelhouse Master Part List 3-18-2026.xlsx"
sku_to_box = {}
for sheet, hdr in [("WHEELS", 1), ("DISCONTINUED WHEELS", 2)]:
    try:
        mx = pd.read_excel(master, sheet_name=sheet, header=hdr, dtype=str).fillna("")
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

print(f"Master SKU -> Box URL: {len(sku_to_box)}")

# --- Build (model,color) -> working /files/ URL from import CSV ---
model_color_to_files = {}
for _, r in imp.iterrows():
    src = r["Image Src"].strip()
    if "/files/" in src:
        model_color_to_files.setdefault((r["_model"], r["_color"]), (src, r["Image Alt Text"]))

print(f"(model,color) -> /files/ URL: {len(model_color_to_files)}")

def resolve_image(handle):
    """Return (url, alt) or (None, None)."""
    sku = handle_to_sku.get(handle, "")
    # 1) master xlsx
    for candidate_sku in [sku, sku.lstrip("Z"), sku[:-1] if len(sku) > 3 else sku]:
        u = sku_to_box.get(candidate_sku)
        if u: return u, ""
    # 2) sibling with /files/ URL
    k = handle_to_meta.get(handle)
    if k and k in model_color_to_files:
        return model_color_to_files[k]
    return None, None

progress_path = HERE / "upload_progress.txt"
log_path = HERE / "upload_log_v2.csv"
uploaded = skipped = failed = 0

with log_path.open("w", newline="", encoding="utf-8") as logf:
    lw = csv.writer(logf); lw.writerow(["handle","product_id","status","url","detail"])
    handles = missing["handle"].tolist()
    for i, h in enumerate(handles):
        pid = handle_to_pid.get(h)
        img, alt = resolve_image(h)
        if not pid or not img:
            lw.writerow([h, pid or "", "SKIP", img or "", "no image resolved"]); skipped += 1; continue
        try:
            resp = requests.post(
                f"https://{STORE}/admin/api/2024-10/products/{pid}/images.json",
                headers=H, json={"image": {"src": img, "alt": alt}}, timeout=60)
            if resp.status_code == 429:
                time.sleep(3)
                resp = requests.post(
                    f"https://{STORE}/admin/api/2024-10/products/{pid}/images.json",
                    headers=H, json={"image": {"src": img, "alt": alt}}, timeout=60)
            if resp.status_code in (200, 201):
                lw.writerow([h, pid, "OK", img, ""]); uploaded += 1
            else:
                lw.writerow([h, pid, f"FAIL_{resp.status_code}", img, resp.text[:150]]); failed += 1
        except Exception as e:
            lw.writerow([h, pid, "EXC", img, str(e)[:150]]); failed += 1
        logf.flush()
        if i % 10 == 0:
            progress_path.write_text(f"{i+1}/{len(handles)} ok={uploaded} skip={skipped} fail={failed}\n", encoding="utf-8")
        time.sleep(0.35)

progress_path.write_text(f"DONE {len(handles)} ok={uploaded} skip={skipped} fail={failed}\n", encoding="utf-8")
print(f"DONE ok={uploaded} skip={skipped} fail={failed}")
