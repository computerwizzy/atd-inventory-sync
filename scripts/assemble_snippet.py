"""Inline the variant_redirect_map.json into the Liquid snippet."""
import json
from pathlib import Path

HERE = Path(r"c:/Users/DELL-i7/Downloads/wheel1_not_in _store")
template = (HERE / "variant_redirect_snippet.liquid").read_text(encoding="utf-8")
data = json.loads((HERE / "variant_redirect_map.json").read_text(encoding="utf-8"))
inlined = template.replace("/*VARIANT_MAP_JSON*/", json.dumps(data))
out = HERE / "variant_redirect_snippet_READY.liquid"
out.write_text(inlined, encoding="utf-8")
print(f"Wrote {out}")
print(f"Size: {len(inlined):,} chars ({len(data)} variant mappings)")
