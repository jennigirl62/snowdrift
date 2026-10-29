"""Build dist/snowdrift.zip: the package as a single importable zip, for places without pip
(e.g. a Snowflake stage). Usage: python tools/build_zip.py, then add the zip to sys.path."""

import zipfile
from pathlib import Path

root = Path(__file__).resolve().parent.parent
src = root / "src" / "snowdrift"
out = root / "dist" / "snowdrift.zip"
out.parent.mkdir(exist_ok=True)
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
    for path in sorted(src.rglob("*")):
        if path.is_file() and "__pycache__" not in path.parts:
            zf.write(path, Path("snowdrift") / path.relative_to(src))
print(f"wrote {out} ({out.stat().st_size // 1024} KB)")
