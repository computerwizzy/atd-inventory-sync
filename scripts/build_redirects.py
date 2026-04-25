"""
Build a Shopify redirects CSV:
  FROM: /products/{old-handle}?variant={variant-id}
  TO:   /products/{new-handle}

Match rule: Variant SKU is unique across both CSVs and is the stable join key.
"""
import csv
import pandas as pd
import re
from pathlib import Path

HERE = Path(r"c:/Users/DELL-i7/Downloads/wheel1_not_in _store")
SRC  = HERE / "method-race-wheels-line.csv"
NEW  = HERE / "method_shopify_import.csv"
OUT  = HERE / "method_redirects.csv"

# Read Variant ID as raw string (it's an int64 that pandas mangles into scientific notation otherwise)
# The safest way is to read the raw CSV line-by-line, or force dtype=str AND disable float parsing.
src = pd.read_csv(SRC, dtype=str, keep_default_na=False)
src["_g"] = (src["Product handle"] != "").cumsum()
src["_parent_handle"] = src.groupby("_g")["Product handle"].transform(
    lambda s: s[s != ""].iloc[0] if (s != "").any() else ""
)

# sanity: Variant ID should already be a long string if read as str
print("sample Variant IDs:", src["Variant ID"].head(3).tolist())
if src["Variant ID"].str.contains("E|e", na=False).any():
    raise SystemExit("Variant ID got mangled to scientific notation — the source CSV itself stores it that way.")

new = pd.read_csv(NEW, dtype=str, keep_default_na=False)

# Join on SKU to get (old_handle, variant_id) -> new_handle
src_slim = src[["_parent_handle", "Variant ID", "Variant SKU"]].rename(
    columns={"_parent_handle": "old_handle", "Variant ID": "variant_id", "Variant SKU": "sku"}
)
new_slim = new[["Handle", "Variant SKU"]].rename(columns={"Handle": "new_handle", "Variant SKU": "sku"})
joined = src_slim.merge(new_slim, on="sku", how="inner")

joined["Redirect from"] = "/products/" + joined["old_handle"] + "?variant=" + joined["variant_id"]
joined["Redirect to"]   = "/products/" + joined["new_handle"]

out = joined[["Redirect from", "Redirect to"]].drop_duplicates()
out.to_csv(OUT, index=False, quoting=csv.QUOTE_MINIMAL)

print(f"Wrote {len(out)} redirects -> {OUT}")
print("\nSample:")
print(out.head(5).to_string(index=False))
