"""For each of the 36 still-missing handles, borrow image from a sibling with the
same Wheel Model + Color that DOES have an image in the import CSV or Shopify.
"""
import csv
import pandas as pd
from pathlib import Path

HERE = Path(r"c:/Users/DELL-i7/Downloads/wheel1_not_in _store")

still = pd.read_csv(HERE / "method_image_STILL_MISSING.csv", dtype=str).fillna("")
imp = pd.read_csv(HERE / "method_shopify_import.csv", dtype=str).fillna("")

MODEL_COL = "Wheel Model (product.metafields.custom.wheel_model)"
COLOR_COL = "Color (product.metafields.custom.color)"

# Normalize color (strip json list brackets/quotes)
def norm_color(c):
    c = (c or "").strip().strip("[]").strip('"').strip()
    return c.lower()

imp["_model"] = imp[MODEL_COL].str.upper().str.strip()
imp["_color"] = imp[COLOR_COL].map(norm_color)
imp["_img"]   = imp["Image Src"].str.strip()

# Build (model, color) -> first non-empty image
key_to_img = {}
for _, r in imp[imp["_img"] != ""].iterrows():
    k = (r["_model"], r["_color"])
    key_to_img.setdefault(k, (r["_img"], r["Image Alt Text"]))

print(f"(model, color) -> image keys: {len(key_to_img)}")

# Handle -> (model, color) from import
handle_meta = {}
for _, r in imp.iterrows():
    handle_meta[r["Handle"]] = (r["_model"], r["_color"])

out = HERE / "method_image_fix_part3_model_color.csv"
recovered = 0
still_missing = []
with out.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["Handle", "Image Src", "Image Position", "Image Alt Text"])
    for h in still["handle"]:
        k = handle_meta.get(h)
        if not k:
            still_missing.append((h, "?", "?")); continue
        hit = key_to_img.get(k)
        if hit:
            w.writerow([h, hit[0], "1", hit[1]]); recovered += 1
        else:
            still_missing.append((h, k[0], k[1]))

print(f"Recovered via model+color: {recovered} / {len(still)}")
print(f"Remaining: {len(still_missing)}")
for h, m, c in still_missing[:20]:
    print(f"  {h}  model={m} color={c}")

# Merge everything into one final file
import pandas as _pd
parts = [HERE/"method_image_fix_import.csv", HERE/"method_image_fix_import_part2.csv", out]
frames = [_pd.read_csv(p, dtype=str).fillna("") for p in parts if p.exists()]
combined = _pd.concat(frames, ignore_index=True).drop_duplicates("Handle", keep="first")
combined.to_csv(HERE/"method_image_fix_FINAL.csv", index=False, quoting=csv.QUOTE_MINIMAL)
print(f"\nFINAL combined: {len(combined)} rows -> method_image_fix_FINAL.csv")
