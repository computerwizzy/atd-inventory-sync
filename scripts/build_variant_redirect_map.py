"""
Build a JSON map of { old_variant_id: new_handle } so a Liquid/JS snippet on the
storefront can redirect old ?variant=... URLs to the matching new product.

Also writes a 113-row product-level CSV (old_parent_handle -> default new_handle)
as a fallback for Shopify's native URL Redirects feature (path-only matching).
"""
import csv
import json
import pandas as pd
from pathlib import Path

HERE = Path(r"c:/Users/DELL-i7/Downloads/wheel1_not_in _store")
SRC  = HERE / "method-race-wheels-line.csv"
NEW  = HERE / "method_shopify_import.csv"
OUT_JSON = HERE / "variant_redirect_map.json"
OUT_PROD_CSV = HERE / "method_redirects_product_level.csv"

src = pd.read_csv(SRC, dtype=str, keep_default_na=False)
src["_g"] = (src["Product handle"] != "").cumsum()
src["_parent_handle"] = src.groupby("_g")["Product handle"].transform(
    lambda s: s[s != ""].iloc[0] if (s != "").any() else ""
)

# Sanity: fail loud if Variant IDs got mangled
sample_vid = src["Variant ID"].iloc[0]
if "E" in sample_vid.upper() or "." in sample_vid:
    raise SystemExit(f"Variant IDs mangled: {sample_vid!r}. Re-export with IDs as plain numbers.")

new = pd.read_csv(NEW, dtype=str, keep_default_na=False)

# Join on SKU
src_slim = src[["_parent_handle", "Variant ID", "Variant SKU"]].rename(
    columns={"_parent_handle": "old_handle", "Variant ID": "variant_id", "Variant SKU": "sku"}
)
new_slim = new[["Handle", "Variant SKU"]].rename(columns={"Handle": "new_handle", "Variant SKU": "sku"})
joined = src_slim.merge(new_slim, on="sku", how="inner")

# --- Variant-level map for the JS snippet ---
variant_map = dict(zip(joined["variant_id"], joined["new_handle"]))
OUT_JSON.write_text(json.dumps(variant_map, indent=2))
print(f"Wrote {len(variant_map)} variant->handle entries -> {OUT_JSON}")

# --- Product-level fallback (one row per old parent handle) ---
# Pick the new handle with the most inventory / largest wheel as default.
# Simple heuristic: pick the first variant alphabetically, which tends to be the smallest size.
# You can refine this later.
prod_level = (joined.sort_values("sku")
                    .drop_duplicates("old_handle", keep="first")
                    .rename(columns={"old_handle":"from","new_handle":"to"}))
prod_level["Redirect from"] = "/products/" + prod_level["from"]
prod_level["Redirect to"]   = "/products/" + prod_level["to"]
prod_level[["Redirect from","Redirect to"]].to_csv(OUT_PROD_CSV, index=False, quoting=csv.QUOTE_MINIMAL)
print(f"Wrote {len(prod_level)} product-level redirects -> {OUT_PROD_CSV}")

print("\nSample variant map entries:")
for vid, h in list(variant_map.items())[:5]:
    print(f"  {vid} -> {h}")
