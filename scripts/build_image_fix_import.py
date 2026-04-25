"""Build a Shopify image-import CSV for the 160 missing-image products.

Output: method_image_fix_import.csv
Columns: Handle, Image Src, Image Position, Image Alt Text
"""
import csv
import pandas as pd
from pathlib import Path

HERE = Path(r"c:/Users/DELL-i7/Downloads/wheel1_not_in _store")

missing = pd.read_csv(HERE / "products_missing_images_yesterday.csv", dtype=str).fillna("")
missing_handles = set(missing["handle"])
print(f"Missing-image handles: {len(missing_handles)}")

imp = pd.read_csv(HERE / "method_shopify_import.csv", dtype=str).fillna("")

# For each missing handle, grab its Image Src from the import CSV
subset = imp[imp["Handle"].isin(missing_handles)].copy()
print(f"Rows in import CSV for those handles: {len(subset)}")

# Most products are 1 row (single variant). Take Image Src / Alt per handle
img_rows = (subset[["Handle", "Image Src", "Image Alt Text"]]
            .drop_duplicates("Handle"))
have_src  = img_rows[img_rows["Image Src"].str.strip() != ""]
no_src    = img_rows[img_rows["Image Src"].str.strip() == ""]
print(f"  with Image Src: {len(have_src)}")
print(f"  without Image Src: {len(no_src)}")

# Write image-fix import
out = HERE / "method_image_fix_import.csv"
with out.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
    w.writerow(["Handle", "Image Src", "Image Position", "Image Alt Text"])
    for _, r in have_src.iterrows():
        w.writerow([r["Handle"], r["Image Src"], "1", r["Image Alt Text"]])

print(f"\nWrote {len(have_src)} rows -> {out}")

# Also report which handles have no image source available at all
if len(no_src):
    no_src_path = HERE / "method_image_fix_NO_SOURCE.csv"
    no_src[["Handle"]].to_csv(no_src_path, index=False)
    print(f"Wrote {len(no_src)} handles with no image source -> {no_src_path}")
    print("\nSample handles missing image source:")
    for h in no_src["Handle"].head(10):
        print(f"  {h}")
