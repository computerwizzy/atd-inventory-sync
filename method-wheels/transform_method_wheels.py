"""
Transform method-race-wheels-line.csv (Shopify export) into a Shopify import CSV
where each row becomes an independent product with 3 separate options:
Size, Bolt Pattern, Offset.
"""
import pandas as pd
import re
import html as _html
from pathlib import Path


def build_spec_html(*, brand, model, finish, diameter, width, bolt_pattern,
                    offset, center_bore, load_rating, structure, part_number):
    """Generate HTML body matching the products_export_1.csv template."""
    def lug(bp):
        m = re.match(r'^(\d+)\s*x', bp or "")
        return m.group(1) if m else ""

    def row(label, val):
        v = "" if val is None or (isinstance(val, float) and pd.isna(val)) else str(val)
        return f"      <tr>\n<td><strong>{label}</strong></td>\n<td>{_html.escape(v)}</td>\n</tr>"

    return (
        "\n    \n"
        "    <h3>Wheel Specifications</h3>\n"
        '    <table border="1" style="border-collapse:collapse;width:100%"><tbody>\n'
        + "\n".join([
            row("Brand", brand),
            row("Model", model),
            row("Finish", finish),
            row("Diameter", diameter),
            row("Width", width),
            row("Lug Count", lug(bolt_pattern)),
            row("Bolt Pattern", bp_dual(bolt_pattern)),
            row("Offset", offset),
            row("Center Bore", center_bore),
            row("Loading Rating", load_rating),
            row("Structure", structure),
            row("Part Number", part_number),
        ])
        + "\n    </tbody></table>\n  "
    )


def extract_spec_lookup(export_df):
    """Build (model, diameter, bolt_pattern) -> {center_bore, load_rating, structure} from export_1."""
    def g(html, field):
        if not isinstance(html, str): return ""
        m = re.search(rf'<strong>{re.escape(field)}</strong></td>\s*<td>(.*?)</td>', html, re.S)
        return m.group(1).strip() if m else ""
    lookup = {}
    for html in export_df["Body (HTML)"].dropna():
        key = (g(html, "Model"), g(html, "Diameter"), g(html, "Bolt Pattern"))
        if not key[0]: continue
        if key not in lookup:
            lookup[key] = {
                "center_bore": g(html, "Center Bore"),
                "load_rating": g(html, "Loading Rating"),
                "structure":   g(html, "Structure") or "1-Piece",
            }
    # also keep a (model, diameter) fallback
    fallback = {}
    for (m, d, _bp), v in lookup.items():
        fallback.setdefault((m, d), v)
    # and model-only fallback
    model_only = {}
    for (m, _d, _bp), v in lookup.items():
        model_only.setdefault(m, v)
    return lookup, fallback, model_only

SRC = Path(r"c:/Users/DELL-i7/Downloads/wheel1_not_in _store/method-race-wheels-line.csv")
ENRICH = Path(r"c:/Users/DELL-i7/Downloads/wheel1_not_in _store/products_export_1.csv")
MASTER_XLSX = Path(r"c:/Users/DELL-i7/Downloads/wheel1_not_in _store/Custom Wheelhouse Master Part List 3-18-2026.xlsx")
OUT = Path(r"c:/Users/DELL-i7/Downloads/wheel1_not_in _store/method_shopify_import.csv")


def load_master_specs():
    """Load the Master Part List xlsx -> dict keyed by Part Number (upper)."""
    if not MASTER_XLSX.exists():
        return {}
    import warnings
    warnings.filterwarnings("ignore")
    # Header rows differ per sheet (WHEELS uses row 1, DISCONTINUED WHEELS uses row 2)
    sheet_headers = {"WHEELS": 1, "DISCONTINUED WHEELS": 2}
    sheets = []
    for s, hdr in sheet_headers.items():
        try:
            df = pd.read_excel(MASTER_XLSX, sheet_name=s, header=hdr, dtype=str)
            df.columns = [str(c).strip() for c in df.columns]
            sheets.append(df)
        except Exception:
            pass
    if not sheets:
        return {}
    df = pd.concat(sheets, ignore_index=True)
    df = df[df["PART NUMBER"].notna()]
    out = {}
    for _, r in df.iterrows():
        pn = str(r["PART NUMBER"]).strip().upper()
        if not pn or pn == "NAN":
            continue
        out[pn] = {
            "model":       str(r.get("MODEL","") or "").strip(),
            "diameter":    str(r.get("WHEEL DIAMETER (IN)","") or "").strip(),
            "width":       str(r.get("WHEEL WIDTH (IN)","") or "").strip(),
            "bolt_pattern":str(r.get("BOLT PATTERN","") or "").strip(),
            "lug_count":   str(r.get("BOLT HOLE","") or "").strip(),
            "offset":      str(r.get("OFFSET (MM)","") or "").strip(),
            "backspace":   str(r.get("BACKSIDE SPACING (IN)","") or "").strip(),
            "hub_bore":    str(r.get("HUB BORE (MM)","") or "").strip(),
            "weight":      str(r.get("WHEEL WEIGHT (LBS)","") or "").strip(),
            "load_rating": str(r.get("MAX LOAD  (LBS)","") or "").strip(),
            "finish":      str(r.get("COLOR/FINISH","") or "").strip(),
            "color":       str(r.get("COLOR FOR WEB SELECTION","") or "").strip(),
            "msrp":        str(r.get("MSRP","") or "").strip(),
            "map":         str(r.get("iMAP PRICE","") or "").strip(),
            "upc":         str(r.get("UPC","") or "").strip(),
        }
    return out


BASE_COLORS = [
    ("BRONZE", "Bronze"), ("COPPER", "Bronze"),
    ("GOLD", "Gold"),
    ("BLACK", "Black"),
    ("WHITE", "White"),
    ("RED", "Red"), ("ORANGE", "Red"),
    ("BLUE", "Blue"),
    ("GREEN", "Green"),
    ("YELLOW", "Yellow"),
    ("PURPLE", "Purple"),
    ("TITANIUM", "Gray"), ("GUN METAL", "Gray"), ("GUNMETAL", "Gray"),
    ("GREY", "Gray"), ("GRAY", "Gray"),
    ("CHROME", "Chrome"),
    ("POLISH", "Polished"),
    ("MACHINED", "Silver"), ("BRUSHED", "Silver"), ("SILVER", "Silver"),
    ("RAW", "Raw"),
]
def derive_color(*candidates):
    """Given any number of color/finish strings, return a single base color."""
    for c in candidates:
        if not c: continue
        u = str(c).upper()
        for keyword, base in BASE_COLORS:
            if keyword in u:
                return base
    return ""


METRIC_TO_IMPERIAL = {
    "108": "4.25", "114.3": "4.5", "120.7": "4.75",
    "127": "5", "139.7": "5.5",
    "152.4": "6", "165.1": "6.5",
    "177.8": "7",
}
def bp_dual(bp):
    """'6x139.7' -> '6x139.7 (6x5.5)'. If pattern has no imperial equivalent, return metric only."""
    if not bp:
        return ""
    s = str(bp).strip().lower().replace("mm","").strip()
    m = re.match(r'^(\d+)\s*x\s*([\d.]+)$', s)
    if not m:
        return s
    studs, pcd = m.group(1), m.group(2)
    imp = METRIC_TO_IMPERIAL.get(pcd)
    return f"{studs}x{pcd} ({studs}x{imp})" if imp else f"{studs}x{pcd}"


IMPERIAL_TO_METRIC = {
    "4.25": "108", "4.5": "114.3", "4.75": "120.7",
    "5": "127", "5.5": "139.7",
    "6": "152.4", "6.5": "165.1",
    "7": "177.8",
}
def to_metric_bp(bp):
    """Convert imperial bolt pattern (e.g. '6x5.5') to metric ('6x139.7'). Leaves already-metric values alone."""
    if not bp:
        return bp
    s = str(bp).strip().lower().replace("mm", "").strip()
    m = re.match(r'^(\d+)\s*x\s*([\d.]+)$', s)
    if not m:
        return s
    studs, pcd = m.group(1), m.group(2)
    if pcd in IMPERIAL_TO_METRIC:
        pcd = IMPERIAL_TO_METRIC[pcd]
    # also catch values like "4.5" stored as "4.50"
    elif pcd.rstrip("0").rstrip(".") in IMPERIAL_TO_METRIC:
        pcd = IMPERIAL_TO_METRIC[pcd.rstrip("0").rstrip(".")]
    return f"{studs}x{pcd}"


def clean_num(v):
    """Strip trailing .0 from numeric-looking strings."""
    if v is None: return ""
    s = str(v).strip()
    if s.endswith(".0"):
        try:
            return str(int(float(s)))
        except ValueError:
            pass
    return s

df = pd.read_csv(SRC, dtype=str)

# Build a proper product-group key: every row with a non-empty Product handle starts a new group.
# The raw Product ID column has gaps that don't align with handle rows, so we can't rely on it.
df["_group"] = df["Product handle"].notna().cumsum()

# Forward-fill the Variant image WITHIN each product group (parent row has the image, variants are blank)
df["Variant image"] = df.groupby("_group")["Variant image"].transform(lambda s: s.ffill().bfill())

# Forward-fill product-level columns (Shopify export leaves them blank on variant rows 2..N)
PRODUCT_COLS = [
    "Product ID","Product metafield value at custom.wheel_diameter",
    "Product metafield value at custom.wheel_width",
    "Product metafield value at custom.hub",
    "Product metafield value at custom.bolt_pattern_2",
    "Product handle","Product title","Product description",
    "Product description without HTML","Product meta title","Product meta description",
    "Product tags","Product taxonomy category id","Product taxonomy category name",
    "Product type","Product vendor","Product template suffix","Product status",
    "Product collections","Product created time","Product published time",
    "Product option 1 name","Product option 2 name","Product option 3 name",
]
for c in PRODUCT_COLS:
    if c in df.columns:
        df[c] = df.groupby("_group")[c].transform(lambda s: s.ffill())
df = df.fillna("")

def parse_option(val):
    """Parse 'Size | Bolt Pattern | Offset/Backspacing [| SKU_or_fitment]' -> (size, bp, offset, backspace, embedded_sku)"""
    if not val:
        return "", "", "", "", ""
    parts = [p.strip() for p in val.split("|")]
    size = parts[0] if len(parts) >= 1 else ""
    bp = parts[1] if len(parts) >= 2 else ""
    offset, backspace = "", ""
    if len(parts) >= 3:
        m = re.match(r'^(-?\d+)\s*/\s*([\d.]+)', parts[2])
        if m:
            offset = m.group(1)
            backspace = m.group(2)
        else:
            m2 = re.match(r'^(-?\d+)', parts[2])
            if m2:
                offset = m2.group(1)
    # 4th part is sometimes an embedded SKU (e.g. "MR10767060825")
    embedded_sku = ""
    if len(parts) >= 4:
        candidate = parts[3].strip()
        if re.match(r'^[A-Z]{1,3}\d{6,}', candidate.upper()):
            embedded_sku = candidate.upper()
    return size, bp, offset, backspace, embedded_sku

# Shopify import columns
OUT_COLS = [
    "Handle","Title","Body (HTML)","Vendor","Product Category","Type","Tags","Published",
    "Option1 Name","Option1 Value","Option2 Name","Option2 Value","Option3 Name","Option3 Value",
    "Variant SKU","Variant Grams","Variant Inventory Tracker","Variant Inventory Qty",
    "Variant Inventory Policy","Variant Fulfillment Service","Variant Price",
    "Variant Compare At Price","Variant Requires Shipping","Variant Taxable",
    "Variant Barcode","Image Src","Image Position","Image Alt Text","Gift Card",
    "SEO Title","SEO Description",
    "Wheel Diameter (product.metafields.custom.wheel_diameter)",
    "Wheel Width (product.metafields.custom.wheel_width)",
    "Hub (product.metafields.custom.hub)",
    "Size (product.metafields.global.size)",
    "Bolt Pattern (product.metafields.global.bolt_pattern)",
    "Offset (product.metafields.global.offset)",
    "Bolt Pattern 2 (product.metafields.custom.bolt_pattern_2)",
    "Backspace (product.metafields.custom.backspace)",
    "Color (product.metafields.custom.color)",
    "Wheel Model (product.metafields.custom.wheel_model)",
    "Variant Image","Variant Weight Unit","Variant Tax Code","Cost per item",
    "Included / United States","Price / United States","Compare At Price / United States",
    "Status"
]

rows = []
for _, r in df.iterrows():
    size, bp, offset, backspace, embedded_sku = parse_option(r.get("Variant option 1 value",""))
    # Normalize bolt pattern to metric (e.g. 6x5.5 -> 6x139.7)
    bp = to_metric_bp(bp)

    # Keep the original parent handle. Each variant becomes its own product, but
    # multiple rows will share the same handle — Shopify needs a unique handle per product.
    # Append the SKU (guaranteed unique per variant) so each independent product has a unique URL
    # while still preserving the original base handle for discoverability/SEO.
    handle_base = r.get("Product handle","")
    sku_slug = (r.get("Variant SKU","") or "").lower()
    handle = f"{handle_base}-{sku_slug}" if (handle_base and sku_slug) else (handle_base or sku_slug)

    # Title: vendor + model + color (parsed from "model | ... | color") + size + bp + offset + sku
    raw_title = r.get("Product title","")
    # Source titles look like "701 |  Bahia Blue" or "305 | NV | Gloss Bahia Blue - Gloss Black Lip"
    title_parts = [p.strip() for p in raw_title.split("|") if p.strip()]
    model = title_parts[0] if title_parts else ""
    color = " ".join(title_parts[1:]) if len(title_parts) > 1 else ""

    sku = r.get("Variant SKU","") or embedded_sku
    bp_display = bp_dual(bp)  # "6x139.7 (6x5.5)"
    title_bits = [
        "Method", model, size, bp_display,
        (offset + "mm" if offset else ""),
        color, sku,
    ]
    title = " ".join(b for b in title_bits if b)

    out = {c: "" for c in OUT_COLS}
    out["Handle"] = handle
    out["Title"] = title
    out["Body (HTML)"] = r.get("Product description","")
    out["Vendor"] = r.get("Product vendor","") or "Method Race Wheels"
    out["Type"] = r.get("Product type","") or "Wheels"
    out["Tags"] = r.get("Product tags","")
    out["Published"] = "TRUE"
    out["Option1 Name"] = "Size"
    out["Option1 Value"] = size or "Default"
    out["Option2 Name"] = "Bolt Pattern" if bp else ""
    out["Option2 Value"] = bp
    out["Option3 Name"] = "Offset" if offset else ""
    out["Option3 Value"] = offset
    out["Variant SKU"] = r.get("Variant SKU","") or embedded_sku
    # Store weight as-is in pounds; Variant Weight Unit is set to "lb" below.
    _w = r.get("Variant weight","")
    try:
        out["Variant Grams"] = str(round(float(_w), 2)) if _w else "0"
    except ValueError:
        out["Variant Grams"] = "0"
    out["Variant Inventory Tracker"] = "shopify"
    out["Variant Inventory Qty"] = "0"
    out["Variant Inventory Policy"] = r.get("Variant inventory policy","") or "deny"
    out["Variant Fulfillment Service"] = "manual"
    out["Variant Price"] = r.get("Variant price","")
    out["Variant Compare At Price"] = r.get("Variant compared price","")
    out["Variant Requires Shipping"] = r.get("Variant requires shipping","") or "TRUE"
    out["Variant Taxable"] = r.get("Variant taxable","") or "TRUE"
    out["Variant Barcode"] = r.get("Variant Barcode","")
    out["Image Src"] = r.get("Variant image","")
    out["Image Position"] = "1" if r.get("Variant image","") else ""
    out["Image Alt Text"] = title
    out["Gift Card"] = "FALSE"
    out["SEO Title"] = r.get("Product meta title","")
    out["SEO Description"] = r.get("Product meta description","")

    # Metafields
    diam_m = re.match(r'^(\d+(?:\.\d+)?)', size)
    diameter = diam_m.group(1) if diam_m else r.get("Product metafield value at custom.wheel_diameter","")
    width_m = re.search(r'x\s*(\d+(?:\.\d+)?)', size)
    width = width_m.group(1) if width_m else r.get("Product metafield value at custom.wheel_width","")

    out["Wheel Diameter (product.metafields.custom.wheel_diameter)"] = diameter
    out["Wheel Width (product.metafields.custom.wheel_width)"] = width
    out["Hub (product.metafields.custom.hub)"] = r.get("Product metafield value at custom.hub","")
    out["Size (product.metafields.global.size)"] = size
    out["Bolt Pattern (product.metafields.global.bolt_pattern)"] = bp.lower() if bp else ""
    out["Offset (product.metafields.global.offset)"] = offset
    # Bolt Pattern 2: same as Bolt Pattern, formatted as single-text list metafield
    bp_l = bp.lower() if bp else ""
    out["Bolt Pattern 2 (product.metafields.custom.bolt_pattern_2)"] = f'["{bp_l}"]' if bp_l else ""
    out["Backspace (product.metafields.custom.backspace)"] = backspace
    # Color: single base color (default Silver for raw/machined), formatted as single-text list metafield
    color_base = derive_color(color) or "Silver"
    out["Color (product.metafields.custom.color)"] = f'["{color_base}"]'
    # Wheel Model: uppercase, e.g. "MR701" or "701" from the parsed model in the source title
    wm = model.strip()
    if wm and not wm.upper().startswith("MR"):
        wm = f"MR{wm}"
    out["Wheel Model (product.metafields.custom.wheel_model)"] = wm.upper()
    # Normalize Shopify unit code (source has "POUNDS" -> "lb")
    _unit = (r.get("Variant weight unit","") or "").strip().lower()
    out["Variant Weight Unit"] = {"pounds":"lb","pound":"lb","lbs":"lb","lb":"lb",
                                  "kilograms":"kg","kg":"kg","grams":"g","g":"g",
                                  "ounces":"oz","oz":"oz"}.get(_unit, "lb")
    out["Cost per item"] = r.get("Variant cost","")
    out["Status"] = r.get("Product status","") or "active"

    rows.append(out)

result = pd.DataFrame(rows, columns=OUT_COLS)

# Enrich from products_export_1.csv by SKU — fill gaps (image, body, seo, cost, compare price)
if ENRICH.exists():
    enr = pd.read_csv(ENRICH, dtype=str)
    enr_by_sku = (enr.dropna(subset=["Variant SKU"])
                     .groupby("Variant SKU")
                     .first()
                     .to_dict(orient="index"))
    # fill-if-empty columns
    FILL_MAP = {
        "Image Src": "Image Src",
        "Image Position": "Image Position",
        "Image Alt Text": "Image Alt Text",
        "SEO Title": "SEO Title",
        "SEO Description": "SEO Description",
        "Cost per item": "Cost per item",
        "Variant Compare At Price": "Variant Compare At Price",
        "Variant Barcode": "Variant Barcode",
    }
    # always-overwrite columns (preferred source)
    OVERWRITE_MAP = {
        "Body (HTML)": "Body (HTML)",
    }
    filled_counts = {k: 0 for k in {**FILL_MAP, **OVERWRITE_MAP}}
    for i, row in result.iterrows():
        sku = row["Variant SKU"]
        if not sku or sku not in enr_by_sku:
            continue
        src_row = enr_by_sku[sku]
        for out_col, enr_col in FILL_MAP.items():
            cur_val = row[out_col]
            enr_val = src_row.get(enr_col)
            if (not cur_val or pd.isna(cur_val)) and enr_val and not pd.isna(enr_val):
                result.at[i, out_col] = enr_val
                filled_counts[out_col] += 1
        for out_col, enr_col in OVERWRITE_MAP.items():
            enr_val = src_row.get(enr_col)
            if enr_val and not pd.isna(enr_val):
                result.at[i, out_col] = enr_val
                filled_counts[out_col] += 1
    print("Enrichment from products_export_1.csv:")
    for k, v in filled_counts.items():
        print(f"  {k}: filled {v} rows")

    # Generate HTML body for rows that still have no export_1 match
    spec_by_mdb, spec_by_md, spec_by_m = extract_spec_lookup(enr)
    master = load_master_specs()
    print(f"  Master xlsx part numbers loaded: {len(master)}")
    generated = 0
    master_hits = 0
    matched_skus = set(enr_by_sku.keys())
    def master_lookup(sku_u):
        """Try exact, then strip 'Z' prefix, then strip trailing suffix, then model-family fallback."""
        if sku_u in master: return master[sku_u]
        if sku_u.startswith("Z") and sku_u[1:] in master: return master[sku_u[1:]]
        import re as _re
        stripped = _re.sub(r'-[A-Z0-9]+$', '', sku_u)
        if stripped != sku_u and stripped in master: return master[stripped]
        if sku_u.startswith("Z"):
            s2 = _re.sub(r'-[A-Z0-9]+$', '', sku_u[1:])
            if s2 in master: return master[s2]
        # Model-family fallback: find any master entry with same MR### prefix to get hub bore etc.
        # Extract just the model number (3 digits max, e.g. MR305 not MR30568...)
        model_prefix = _re.match(r'^Z?(MR\d{2,3})', sku_u)
        if model_prefix:
            prefix = model_prefix.group(1)
            siblings = [v for k, v in master.items() if k.startswith(prefix)]
            if siblings:
                base = dict(siblings[0])
                base["bolt_pattern"] = ""
                base["offset"] = ""
                base["backspace"] = ""
                # Build a lug-count -> hub_bore map from siblings so we can match correctly later
                lug_hub = {}
                for s in siblings:
                    bp_s = s.get("bolt_pattern","")
                    hub_s = s.get("hub_bore","")
                    lm = _re.match(r'^(\d+)', bp_s)
                    if lm and hub_s:
                        lug_hub[lm.group(1)] = hub_s
                base["_lug_hub_map"] = lug_hub
                base["hub_bore"] = ""  # will be resolved per-row below
                return base
        return None

    # Apply master xlsx metafield backfill to ALL rows (even export_1-matched) — fills Hub Bore, UPC, Backspace, etc.
    master_meta_hits = 0
    for i, row in result.iterrows():
        sku_u = (row["Variant SKU"] or "").strip().upper()
        m = master_lookup(sku_u)
        if not m:
            continue
        master_meta_hits += 1
        if not row["Wheel Diameter (product.metafields.custom.wheel_diameter)"]:
            result.at[i, "Wheel Diameter (product.metafields.custom.wheel_diameter)"] = clean_num(m["diameter"])
        if not row["Wheel Width (product.metafields.custom.wheel_width)"]:
            result.at[i, "Wheel Width (product.metafields.custom.wheel_width)"] = clean_num(m["width"])
        if not row["Hub (product.metafields.custom.hub)"]:
            hub = m.get("hub_bore","")
            if not hub and m.get("_lug_hub_map"):
                # Derive hub bore from lug count of this row's bolt pattern
                bp_row = row["Option2 Value"] or row["Bolt Pattern (product.metafields.global.bolt_pattern)"]
                lm = re.match(r'^(\d+)', bp_row)
                if lm:
                    hub = m["_lug_hub_map"].get(lm.group(1), "")
            if hub:
                result.at[i, "Hub (product.metafields.custom.hub)"] = clean_num(hub)
        if not row["Backspace (product.metafields.custom.backspace)"]:
            result.at[i, "Backspace (product.metafields.custom.backspace)"] = clean_num(m["backspace"])
        if not row["Variant Barcode"] and m["upc"]:
            result.at[i, "Variant Barcode"] = m["upc"]
        # Fill Option2 (Bolt Pattern) and Option3 (Offset) from master when blank
        if not row["Option2 Value"] and m["bolt_pattern"]:
            master_bp = to_metric_bp(m["bolt_pattern"])
            result.at[i, "Option2 Value"] = master_bp
            result.at[i, "Option2 Name"] = "Bolt Pattern"
            result.at[i, "Bolt Pattern (product.metafields.global.bolt_pattern)"] = master_bp.lower()
            bp_l = master_bp.lower()
            result.at[i, "Bolt Pattern 2 (product.metafields.custom.bolt_pattern_2)"] = f'["{bp_l}"]'
        if not row["Option3 Value"] and m["offset"]:
            master_off = clean_num(m["offset"])
            result.at[i, "Option3 Value"] = master_off
            result.at[i, "Option3 Name"] = "Offset"
            result.at[i, "Offset (product.metafields.global.offset)"] = master_off
        if not row["Backspace (product.metafields.custom.backspace)"] and m["backspace"]:
            result.at[i, "Backspace (product.metafields.custom.backspace)"] = clean_num(m["backspace"])
        # Price policy: use iMAP, fall back to MSRP; Compare At Price = MSRP when MAP < MSRP
        def _num(v):
            s = str(v or "").replace(",", "").strip().lower()
            if not s or s == "nan":
                return None
            try:
                f = float(s)
                return None if (f != f) else f  # reject NaN
            except Exception:
                return None
        map_p = _num(m.get("map"))
        msrp_p = _num(m.get("msrp"))
        target = map_p if map_p else msrp_p
        if target is not None:
            result.at[i, "Variant Price"] = f"{target:g}"
            if msrp_p and map_p and msrp_p > map_p:
                result.at[i, "Variant Compare At Price"] = f"{msrp_p:g}"
            else:
                result.at[i, "Variant Compare At Price"] = ""
        # Color: prefer master's "COLOR FOR WEB SELECTION" (already a clean base color)
        master_color = derive_color(m.get("color"), m.get("finish"))
        if master_color:
            result.at[i, "Color (product.metafields.custom.color)"] = f'["{master_color}"]'
        # Wheel Model: keep only the MR### code, drop descriptive suffix (e.g. "MR305 NV" -> "MR305")
        if m.get("model"):
            mm = re.match(r'(MR\w*?\d+[A-Z]?)', m["model"].upper())
            if mm:
                result.at[i, "Wheel Model (product.metafields.custom.wheel_model)"] = mm.group(1)
    print(f"  Master xlsx metafield enrichment applied to {master_meta_hits} rows")

    # Rebuild Handle and Title for rows where Option2/Option3 were filled from master
    for i, row in result.iterrows():
        bp2 = row["Option2 Value"]
        off3 = row["Option3 Value"]
        sku = row["Variant SKU"]
        size = row["Option1 Value"]
        # Rebuild handle: base-handle is everything before the sku slug
        sku_slug = sku.lower()
        base_handle = row["Handle"].replace(f"-{sku_slug}", "").replace(sku_slug, "")
        new_handle = f"{base_handle}-{sku_slug}" if base_handle else sku_slug
        if new_handle != row["Handle"]:
            result.at[i, "Handle"] = new_handle
        # Rebuild title only if it's missing bp or offset info
        title = row["Title"]
        bp_display = bp_dual(bp2)
        off_str = (off3 + "mm") if off3 else ""
        # Check if title already contains the bp and offset
        if bp2 and bp2 not in title:
            # Rebuild: "Method <model> <size> <bp_display> <offset>mm <color> <sku>"
            parts = title.split()
            # find where size starts in title (after "Method <model>")
            try:
                size_idx = next(j for j, p in enumerate(parts) if size.replace("x","") in p.replace("x",""))
                prefix = " ".join(parts[:size_idx])
                suffix_parts = [p for p in parts[size_idx:] if p not in (size,) and "x" not in p and p != sku]
                color_str = " ".join(p for p in suffix_parts if not re.match(r'^-?\d+mm$', p))
                new_title = " ".join(filter(None, [prefix, size, bp_display, off_str, color_str, sku]))
                result.at[i, "Title"] = new_title
                result.at[i, "Image Alt Text"] = new_title
            except StopIteration:
                pass

    # Last-resort: parse offset from SKU encoding when still blank
    # Method SKU encodes offset in last 3 digits before optional letter suffix
    # e.g. MR305 78 587 200 -> offset 0, MR701 79 060 612 N -> offset -12
    def offset_from_sku(sku_raw):
        m = re.search(r'(\d{3})[A-Z]*$', sku_raw.upper())
        if not m: return ""
        raw = m.group(1)
        val = int(raw)
        if val == 0 or (1 <= val <= 99): return str(val)
        if 600 <= val <= 699: return str(-(val - 600))
        if 200 <= val <= 299: return str(val - 200)
        return ""

    for i, row in result.iterrows():
        if not row["Option3 Value"]:
            off = offset_from_sku(row["Variant SKU"])
            if off != "":
                result.at[i, "Option3 Value"] = off
                result.at[i, "Option3 Name"] = "Offset"
                result.at[i, "Offset (product.metafields.global.offset)"] = off
        if not row["Backspace (product.metafields.custom.backspace)"]:
            # Backspace can't be reliably derived from SKU — leave blank rather than guess

            pass

    for i, row in result.iterrows():
        if row["Variant SKU"] in matched_skus:
            continue  # already got real body from export_1
        sku_u = (row["Variant SKU"] or "").strip().upper()
        m = master_lookup(sku_u)
        if m:
            master_hits += 1
        model_raw = row["Title"].split(" ", 2)[1] if len(row["Title"].split(" ", 2)) >= 2 else ""
        diameter = row["Wheel Diameter (product.metafields.custom.wheel_diameter)"]
        width = row["Wheel Width (product.metafields.custom.wheel_width)"]
        bp = row["Bolt Pattern (product.metafields.global.bolt_pattern)"]
        offset = row["Offset (product.metafields.global.offset)"]
        sku = row["Variant SKU"]
        # parse finish/color out of title: "Method <model> <size> <bp> <offset>mm <color...> <sku>"
        t = row["Title"]
        m_fin = re.search(r'mm\s+(.+?)\s+' + re.escape(sku) + r'\s*$', t)
        finish = m_fin.group(1) if m_fin else ""
        # Find richest matching spec data
        # Try (model, diameter, bp) first; the model from title may differ in formatting from export_1's
        spec = None
        for key in [(f"Bead Grip MR{model_raw}", diameter, bp),
                    (f"MR{model_raw}", diameter, bp),
                    (model_raw, diameter, bp)]:
            if key in spec_by_mdb:
                spec = spec_by_mdb[key]; break
        if not spec:
            for key in [(f"Bead Grip MR{model_raw}", diameter),
                        (f"MR{model_raw}", diameter),
                        (model_raw, diameter)]:
                if key in spec_by_md:
                    spec = spec_by_md[key]; break
        if not spec:
            for key in [f"Bead Grip MR{model_raw}", f"MR{model_raw}", model_raw]:
                if key in spec_by_m:
                    spec = spec_by_m[key]; break
        spec = spec or {"center_bore": "", "load_rating": "", "structure": "1-Piece"}

        # prefer master specs when present
        if m:
            model_name = m["model"] or (f"MR{model_raw}" if model_raw and not model_raw.lower().startswith("mr") else model_raw)
            finish_use = m["finish"] or finish
            diameter_use = clean_num(m["diameter"]) or diameter
            width_use = clean_num(m["width"]) or width
            bp_use = to_metric_bp(m["bolt_pattern"]) or bp
            offset_use = clean_num(m["offset"]) or offset
            center_bore = clean_num(m["hub_bore"]) or spec["center_bore"]
            load_rating = clean_num(m["load_rating"]) or spec["load_rating"]
        else:
            model_name = f"MR{model_raw}" if model_raw and not model_raw.lower().startswith("mr") else model_raw
            finish_use = finish
            diameter_use = diameter
            width_use = width
            bp_use = bp
            offset_use = offset
            center_bore = spec["center_bore"]
            load_rating = spec["load_rating"]

        html = build_spec_html(
            brand="Method",
            model=model_name,
            finish=finish_use,
            diameter=diameter_use,
            width=width_use,
            bolt_pattern=bp_use,
            offset=offset_use,
            center_bore=center_bore,
            load_rating=load_rating,
            structure=spec["structure"],
            part_number=sku,
        )
        result.at[i, "Body (HTML)"] = html
        generated += 1
    print(f"  Body (HTML) generated from template for {generated} unmatched rows")
    print(f"    of which {master_hits} used real specs from the Master Part List xlsx")

result.to_csv(OUT, index=False)

print(f"Wrote {len(result)} rows -> {OUT}")
print(f"Unique handles (independent products): {result['Handle'].nunique()}")
print(f"Source rows: {len(df)}")
print("\nSample:")
print(result[["Handle","Title","Option1 Value","Option2 Value","Option3 Value","Variant SKU"]].head(8).to_string())
