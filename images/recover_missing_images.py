"""Recover image sources for the 92 no-image handles using:
  1. Source CSV (method-race-wheels-line.csv) by variant SKU -> parent handle -> image
  2. Master xlsx IMAGE LINK columns by SKU
"""
import csv
import pandas as pd
from pathlib import Path

HERE = Path(r"c:/Users/DELL-i7/Downloads/wheel1_not_in _store")

no_src = pd.read_csv(HERE / "method_image_fix_NO_SOURCE.csv", dtype=str).fillna("")
imp = pd.read_csv(HERE / "method_shopify_import.csv", dtype=str).fillna("")

# Handle -> SKU (from new import)
handle_to_sku = dict(zip(imp["Handle"], imp["Variant SKU"]))

# --- Build SKU -> image from SOURCE CSV ---
src = pd.read_csv(HERE / "method-race-wheels-line.csv", dtype=str, keep_default_na=False)
# Forward-fill parent handle & image down each group
src["_g"] = (src["Product handle"] != "").cumsum()
src["_parent_handle"] = src.groupby("_g")["Product handle"].transform(
    lambda s: s[s != ""].iloc[0] if (s != "").any() else "")
img_col = "Variant image" if "Variant image" in src.columns else None
print(f"Source image column: {img_col}")
# Parent image = first non-empty variant image within each product group
if img_col:
    src["_parent_image"] = src.groupby("_g")[img_col].transform(
        lambda s: s[s != ""].iloc[0] if (s != "").any() else "")
else:
    src["_parent_image"] = ""

sku_to_img_src = {}
for _, r in src.iterrows():
    sku = r.get("Variant SKU", "").strip()
    # Prefer variant image, fall back to parent
    img = (r[img_col] if img_col else "") or r["_parent_image"]
    if sku and img:
        sku_to_img_src.setdefault(sku, img)

print(f"Source SKU->image mappings: {len(sku_to_img_src)}")

# --- Build SKU -> image from MASTER xlsx ---
master_path = HERE / "Custom Wheelhouse Master Part List 3-18-2026.xlsx"
sku_to_img_master = {}
if master_path.exists():
    for sheet, hdr in [("WHEELS", 1), ("DISCONTINUED WHEELS", 2)]:
        try:
            mx = pd.read_excel(master_path, sheet_name=sheet, header=hdr, dtype=str).fillna("")
        except Exception as e:
            print(f"skip {sheet}: {e}"); continue
        # Find SKU + IMAGE columns
        sku_col = next((c for c in mx.columns if "part" in str(c).lower() and "number" in str(c).lower()), None)
        if not sku_col:
            sku_col = next((c for c in mx.columns if str(c).strip().lower() in ("sku","part #","part#")), None)
        img_cols = [c for c in mx.columns if "image" in str(c).lower() or "photo" in str(c).lower()]
        print(f"{sheet}: sku_col={sku_col!r} img_cols={img_cols}")
        if not sku_col or not img_cols:
            continue
        for _, r in mx.iterrows():
            sku = str(r[sku_col]).strip()
            if not sku: continue
            for ic in img_cols:
                val = str(r[ic]).strip()
                if val and val.lower().startswith(("http", "www")):
                    sku_to_img_master.setdefault(sku, val)
                    break

print(f"Master SKU->image mappings: {len(sku_to_img_master)}")

# --- Resolve the 92 handles ---
out = HERE / "method_image_fix_import_part2.csv"
still_missing = []
found = 0
with out.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["Handle","Image Src","Image Position","Image Alt Text"])
    for h in no_src["Handle"]:
        sku = handle_to_sku.get(h, "")
        img = sku_to_img_src.get(sku) or sku_to_img_master.get(sku)
        # Try Z-prefix / suffix trims
        if not img and sku:
            if sku.startswith("Z"):
                img = sku_to_img_src.get(sku[1:]) or sku_to_img_master.get(sku[1:])
            if not img and len(sku) > 2:
                img = sku_to_img_src.get(sku[:-1]) or sku_to_img_master.get(sku[:-1])
        if img:
            w.writerow([h, img, "1", ""]); found += 1
        else:
            still_missing.append((h, sku))

print(f"\nRecovered: {found} / {len(no_src)}")
print(f"Still missing: {len(still_missing)}")
if still_missing:
    (HERE / "method_image_STILL_MISSING.csv").write_text(
        "handle,sku\n" + "\n".join(f"{h},{s}" for h, s in still_missing), encoding="utf-8")
    print("Sample still-missing:")
    for h, s in still_missing[:10]:
        print(f"  {h}  sku={s}")
