import contextlib
import io
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class ZipTests(unittest.TestCase):
    def test_zip_imports_and_demo_runs(self):
        """The single-file zip (for Snowflake stages) must work with nothing but the zip on sys.path."""
        with tempfile.TemporaryDirectory() as tmp:
            out = subprocess.run(
                [sys.executable, str(ROOT / "tools" / "build_zip.py")], capture_output=True, text=True, check=True
            ).stdout
            self.assertIn("snowdrift.zip", out)
            code = (
                "import sys, io, contextlib; sys.path.insert(0, sys.argv[1]); import snowdrift.cli as c\n"
                "buf = io.StringIO()\n"
                "with contextlib.redirect_stdout(buf): c.main(['demo'])\n"
                "print('possible_rename on CUSTOMERS.PHONE' in buf.getvalue())"
            )
            res = subprocess.run(
                [sys.executable, "-I", "-c", code, str(ROOT / "dist" / "snowdrift.zip")],
                capture_output=True, text=True, cwd=tmp,
            )  # fmt: skip
        self.assertEqual(res.stdout.strip(), "True", res.stderr[-2000:])


if __name__ == "__main__":
    with contextlib.redirect_stdout(io.StringIO()):
        unittest.main()
