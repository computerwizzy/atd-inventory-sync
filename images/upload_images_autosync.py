"""Pull 3-angle images from AutoSync Studio API and upload to Shopify products."""
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
AS_BASE = "https://api.autosyncstudio.com"
IMG_BASE = "https://wheels.autosyncstudio.com/png/"   # use PNG (highest quality)

missing = pd.read_csv(HERE / "products_missing_images_yesterday.csv", dtype=str).fillna("")
imp = pd.read_csv(HERE / "method_shopify_import.csv", dtype=str).fillna("")

MODEL_COL = "Wheel Model (product.metafields.custom.wheel_model)"
COLOR_COL = "Color (product.metafields.custom.color)"
def norm_color(c): return (c or "").strip().strip("[]").strip('"').strip()

handle_to_sku   = dict(zip(imp["Handle"], imp["Variant SKU"]))
handle_to_model = dict(zip(imp["Handle"], imp[MODEL_COL].str.upper().str.strip()))
handle_to_color = dict(zip(imp["Handle"], imp[COLOR_COL].map(norm_color)))
handle_to_title = dict(zip(imp["Handle"], imp["Title"]))
handle_to_pid   = dict(zip(missing["handle"], missing["product_id"]))

sess = requests.Session()

def swap_ext(p, new_ext=".png"):
    base, _ = os.path.splitext(p)
    return base + new_ext

def _wheel_to_angles(w):
    out = []
    for ang in ("Img0001", "Img0002", "Img0003"):
        rel = (w.get(ang) or "").strip()
        if rel:
            out.append((ang, IMG_BASE + swap_ext(rel, ".png")))
    return out

def lookup_by_pn(pn):
    params = {"key": AS_KEY, "f-pn": pn,
              "i-img0001":"true","i-img0002":"true","i-img0003":"true"}
    r = sess.get(AS_BASE + "/wheels", params=params, timeout=30)
    if r.status_code != 200: return [], ""
    ws = r.json().get("Wheels", [])
    if not ws: return [], ""
    return _wheel_to_angles(ws[0]), f"pn={pn}"

def lookup_by_query(q):
    params = {"key": AS_KEY, "f-query": q, "f-brand": "Method",
              "i-img0001":"true","i-img0002":"true","i-img0003":"true"}
    r = sess.get(AS_BASE + "/wheels", params=params, timeout=30)
    if r.status_code != 200: return [], ""
    ws = r.json().get("Wheels", [])
    if not ws: return [], ""
    # Prefer the first hit that has 0001
    for w in ws:
        angles = _wheel_to_angles(w)
        if angles: return angles, f"query={q} pn={w.get('Pn')}"
    return [], ""

def lookup_images(sku, model, color):
    # 1) exact PN, and common SKU variants
    pns = [sku]
    if sku.startswith("Z"): pns.append(sku[1:])
    if len(sku) > 3: pns.append(sku[:-1])
    for pn in pns:
        imgs, src = lookup_by_pn(pn)
        if imgs: return imgs, src
    # 2) full-text query by model + color
    model_num = model.replace("MR","").lstrip("0") if model else ""
    if model_num and color:
        q = f"MR{model_num} {color}"
        imgs, src = lookup_by_query(q)
        if imgs: return imgs, src
    # 3) just model
    if model_num:
        imgs, src = lookup_by_query(f"MR{model_num}")
        if imgs: return imgs, src
    return [], ""

def upload_to_shopify(pid, url, position):
    payload = {"image": {"src": url, "position": position}}
    r = sess.post(f"https://{SHOP_STORE}/admin/api/2024-10/products/{pid}/images.json",
                  headers=SHOP_H, json=payload, timeout=60)
    if r.status_code == 429:
        time.sleep(3)
        r = sess.post(f"https://{SHOP_STORE}/admin/api/2024-10/products/{pid}/images.json",
                      headers=SHOP_H, json=payload, timeout=60)
    return r

log_path = HERE / "upload_log_autosync.csv"
progress_path = HERE / "upload_progress.txt"
handles = missing["handle"].tolist()

prod_ok = prod_partial = prod_none = 0
img_ok = img_fail = 0

with log_path.open("w", newline="", encoding="utf-8") as f:
    lw = csv.writer(f)
    lw.writerow(["handle","product_id","sku","pn_tried","angle","url","status","detail"])
    for i, h in enumerate(handles):
        pid = handle_to_pid.get(h)
        sku = handle_to_sku.get(h, "")
        if not pid:
            lw.writerow([h,"",sku,"","","","SKIP","no pid"]); continue

        model = handle_to_model.get(h, "")
        color = handle_to_color.get(h, "")
        imgs, hit_pn = lookup_images(sku, model, color)
        if not imgs:
            lw.writerow([h, pid, sku, f"model={model} color={color}", "", "", "NO_MATCH", ""])
            prod_none += 1
            if i % 5 == 0:
                progress_path.write_text(
                    f"{i+1}/{len(handles)} prod_ok={prod_ok} partial={prod_partial} none={prod_none} imgs_ok={img_ok}\n",
                    encoding="utf-8")
            continue

        pos = 0
        this_ok = 0
        for angle, url in imgs:
            pos += 1
            r = upload_to_shopify(pid, url, pos)
            if r.status_code in (200, 201):
                lw.writerow([h, pid, sku, hit_pn, angle, url, "OK", ""])
                img_ok += 1; this_ok += 1
            else:
                lw.writerow([h, pid, sku, hit_pn, angle, url, f"FAIL_{r.status_code}", r.text[:120]])
                img_fail += 1
            time.sleep(0.3)
        if this_ok == len(imgs):
            prod_ok += 1
        elif this_ok > 0:
            prod_partial += 1
        else:
            prod_none += 1
        f.flush()
        if i % 5 == 0:
            progress_path.write_text(
                f"{i+1}/{len(handles)} prod_ok={prod_ok} partial={prod_partial} none={prod_none} imgs_ok={img_ok}\n",
                encoding="utf-8")

progress_path.write_text(
    f"DONE {len(handles)} prod_ok={prod_ok} partial={prod_partial} none={prod_none} imgs_ok={img_ok} imgs_fail={img_fail}\n",
    encoding="utf-8")
print(f"DONE prod_ok={prod_ok} partial={prod_partial} none={prod_none} imgs_ok={img_ok} imgs_fail={img_fail}")
