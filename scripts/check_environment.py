"""Check migration prerequisites without importing or executing robot workflows."""

import ast
import importlib
from importlib.metadata import version
from io import BytesIO
from pathlib import Path
import platform
import struct
import sys
import tokenize


def main() -> None:
    if (
        sys.version_info[:2] != (3, 12)
        or platform.python_implementation() != "CPython"
        or sys.platform != "win32"
        or struct.calcsize("P") != 8
    ):
        raise SystemExit("Required: CPython 3.12, 64-bit Windows. Run with uv run --locked.")

    print(f"Runtime: {sys.version.split()[0]} ({struct.calcsize('P') * 8}-bit)")
    for module, distribution in (
        ("pandas", "pandas"),
        ("numpy", "numpy"),
        ("openpyxl", "openpyxl"),
        ("xlrd", "xlrd"),
        ("pyodbc", "pyodbc"),
        ("serial", "pyserial"),
        ("requests", "requests"),
        ("PyInstaller", "pyinstaller"),
    ):
        importlib.import_module(module)
        print(f"Import OK: {distribution} {version(distribution)}")

    import tkinter
    import pandas as pd
    import pyodbc

    # Initializes Tcl without opening a window or touching any hardware.
    print(f"Tcl: {tkinter.Tcl().eval('info patchlevel')}; Tk: {tkinter.TkVersion}")
    print(f"64-bit ODBC drivers: {pyodbc.drivers()}")

    # Exercise the Excel engine and dataframe operations used by setup GUIs.
    workbook = BytesIO()
    source = pd.DataFrame({"Destination": ["A1", "B1"], "Source": ["T", "Culture"], "Vol": [10, 20]})
    source.to_excel(workbook, index=False, startrow=1, engine="openpyxl")
    workbook.seek(0)
    actual = pd.read_excel(workbook, header=1).dropna(subset=["Destination", "Source"])
    pd.testing.assert_frame_equal(actual, source)
    assignments = actual["Source"].apply(lambda value: "MediaCtrl" if value == "T" else "Cells")
    if assignments.tolist() != ["MediaCtrl", "Cells"]:
        raise RuntimeError("Excel well assignment smoke check failed")
    print("Excel round-trip and well assignment: OK")

    root = Path(__file__).resolve().parents[1]
    count = 0
    for directory in ("Labwares", "VENUS Python Modules", "TestingTools", "SQL Database", "scripts"):
        for path in sorted((root / directory).rglob("*.py")):
            with tokenize.open(path) as source_file:
                source_text = source_file.read()
            ast.parse(source_text, filename=str(path))
            compile(source_text, str(path), "exec")
            count += 1
    print(f"Python 3.12 syntax/compilation: {count} source files OK (not executed)")
    print("Database connectivity, GUI interaction, and robot behavior require target-PC validation.")


if __name__ == "__main__":
    main()
