"""Zip src/eproc_metacnj into scripts/eproc_metacnj.zip for spark-submit --py-files.

Run from anywhere:  python eproc2g/build_zip.py
"""

import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
OUT = ROOT / "scripts" / "eproc_metacnj.zip"


def main() -> None:
    with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted((SRC / "eproc_metacnj").rglob("*.py")):
            zf.write(path, path.relative_to(SRC).as_posix())
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
