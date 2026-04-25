"""For each remaining handle, search Shopify for siblings with same Wheel Model + Color
metafields and borrow their image.
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

TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
STORE = os.environ["SHOPIFY_STORE_URL"]
H = {"X-Shopify-Access-Token": TOKEN}

imp = pd.read_csv(HERE / "method_shopify_import.csv", dtype=str).fillna("")
MODEL_COL = "Wheel Model (product.metafields.custom.wheel_model)"
COLOR_COL = "Color (product.metafields.custom.color)"

def norm_color(c):
    return (c or "").strip().strip("[]").strip('"').strip().lower()

imp["_model"] = imp[MODEL_COL].str.upper().str.strip()
imp["_color"] = imp[COLOR_COL].map(norm_color)
handle_meta = dict(zip(imp["Handle"], zip(imp["_model"], imp["_color"])))

# Already-handled handles
done_handles = set()
for fn in ["method_image_fix_import.csv", "method_image_fix_import_part2.csv",
           "method_image_fix_part3_model_color.csv"]:
    p = HERE / fn
    if p.exists():
        done_handles.update(pd.read_csv(p, dtype=str).fillna("")["Handle"])

# Remaining handles (no image yet)
missing_orig = set(pd.read_csv(HERE / "products_missing_images_yesterday.csv", dtype=str).fillna("")["handle"])
remaining = [h for h in missing_orig if h not in done_handles]
print(f"Remaining without image: {len(remaining)}")

# Group remaining by (model, color)
groups = {}
for h in remaining:
    k = handle_meta.get(h)
    if k: groups.setdefault(k, []).append(h)

print(f"Unique (model, color) groups to resolve: {len(groups)}")

# For each group, find a live Shopify product with that model+color that has an image.
# Strategy: query products by handle prefix matching the model number.
def find_sibling_image(model, color):
    # model like "MR103" -> search handles containing "103-"
    num = model.replace("MR","").lstrip("0") or model.replace("MR","")
    prefixes = [f"{num}-"]
    for prefix in prefixes:
        # Scan our own import for candidate handles with same model+color that DO have an image
        cand = imp[(imp["_model"] == model) & (imp["_color"] == color) & (imp["Image Src"].str.strip() != "")]
        if not cand.empty:
            return cand.iloc[0]["Image Src"], cand.iloc[0]["Image Alt Text"]
    # Fallback: query Shopify for products with this model handle prefix and any image
    url = f"https://{STORE}/admin/api/2024-10/products.json"
    # Try handle prefix via products?handle= (exact only) — instead scan by title via query param is not supported.
    # Use product search endpoint? Not available. We'll scan pages of products filtered by product_type or vendor.
    # Simpler: page through products with vendor=Method-Race-Wheels and filter client-side.
    params = {"vendor": "Method Race Wheels", "fields": "id,handle,title,images", "limit": 250}
    sess = requests.Session(); sess.headers.update(H)
    since_id = 0
    needle_num = num
    best = None
    for _ in range(20):
        p = dict(params); p["since_id"] = since_id
        r = sess.get(url, params=p, timeout=30)
        if r.status_code == 429: time.sleep(2); continue
        items = r.json().get("products", [])
        if not items: break
        for it in items:
            since_id = max(since_id, it["id"])
            hnd = it.get("handle","").lower()
            if needle_num in hnd and it.get("images"):
                # crude color match: color word in handle
                color_words = {
                    "silver": ["machined","silver","polished"],
                    "black":  ["black"],
                    "bronze": ["bronze"],
                    "gray":   ["gray","grey","titanium"],
                    "gold":   ["gold"],
                    "blue":   ["blue"],
                }.get(color, [color])
                if any(w in hnd for w in color_words):
                    return it["images"][0]["src"], it.get("title","")
                if best is None:
                    best = (it["images"][0]["src"], it.get("title",""))
        if len(items) < 250: break
    return best if best else (None, None)

out = HERE / "method_image_fix_part4_shopify.csv"
recovered = 0
still = []
with out.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["Handle","Image Src","Image Position","Image Alt Text"])
    for (model, color), handles in groups.items():
        src, alt = find_sibling_image(model, color)
        if src:
            for h in handles:
                w.writerow([h, src, "1", alt or ""]); recovered += 1
            print(f"  {model} / {color}: using {src[:60]}... for {len(handles)} handle(s)")
        else:
            for h in handles:
                still.append((h, model, color))
            print(f"  {model} / {color}: NO MATCH in Shopify ({len(handles)} handles)")

print(f"\nRecovered: {recovered}   Still missing: {len(still)}")

# Combine all
parts = [HERE/"method_image_fix_import.csv", HERE/"method_image_fix_import_part2.csv",
         HERE/"method_image_fix_part3_model_color.csv", out]
frames = [pd.read_csv(p, dtype=str).fillna("") for p in parts if p.exists() and p.stat().st_size > 40]
combined = pd.concat(frames, ignore_index=True).drop_duplicates("Handle", keep="first")
combined.to_csv(HERE/"method_image_fix_FINAL.csv", index=False, quoting=csv.QUOTE_MINIMAL)
print(f"FINAL: {len(combined)} rows -> method_image_fix_FINAL.csv")

if still:
    (HERE/"method_image_STILL_MISSING.csv").write_text(
        "handle,model,color\n" + "\n".join(f"{h},{m},{c}" for h,m,c in still), encoding="utf-8")
