import pyodbc
import os
import sys
import random
import pandas as pd
import argparse
import subprocess
from datetime import datetime

# === Logging Setup ===
log_dir = r"C:\Python Log"
os.makedirs(log_dir, exist_ok=True)
script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(log_dir, f"{script_name}_{timestamp}.log")

def log(msg):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{now}] {msg}\n")

# === Parse Arguments ===
parser = argparse.ArgumentParser()
parser.add_argument("PlateBarcode", type=str, help="Plate Barcode Identifier")
parser.add_argument(
    "--test-mode",
    action="store_true",
    help="If set, generate random raw files with '_test' suffix (preserving presence/absence of extension) and use them for BCP."
)
args = parser.parse_args()
PlateBarcode = args.PlateBarcode
TEST_MODE = args.test_mode

# Gen5 raw data paths (often no extension)
RawDataPath_FlEx482Em510 = r"C:\\Program Files\\HAMILTON\\Gen5Data\\FlEx482Em510_data"
RawDataPath_FlEx587Em611 = r"C:\\Program Files\\HAMILTON\\Gen5Data\\FlEx587Em611_data"

# === Helpers ===
def add_test_suffix_preserve_ext(path: str) -> str:
    """
    Add '_test' before the extension; if there is no extension, just append '_test'.
    Examples:
      '..._data' -> '..._data_test'
      '..._data.txt' -> '..._data_test.txt'
    """
    root, ext = os.path.splitext(path)
    return f"{root}_test{ext}"

def wells_96():
    rows = ["A","B","C","D","E","F","G","H"]
    cols = range(1,13)
    return [f"{r}{c}" for c in cols for r in rows]

# === Database Connection ===
def establish_connection():
    try:
        conn_str = (
            "DRIVER={ODBC Driver 11 for SQL Server};"
            "SERVER=LOCALHOST\\HAMILTON;"
            "DATABASE=EvoYeast;"
            "UID=Hamilton;"
            "PWD=mkdpw:V43;"
            "Trust_Connection=no;"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        log(f"ERROR: DB connection failed: {e}")
        sys.exit(1)

# === Get Latest RunID ===
def get_runID():
    try:
        conn = establish_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 1 RunGUID FROM HamiltonVectorDB.dbo.HxRun ORDER BY StartTime DESC")
        row = cursor.fetchone()
        conn.close()
        if not row:
            log("ERROR: No RunGUID found.")
            sys.exit(1)
        run_id = row[0]
        log(f"Retrieved RunGUID: {run_id}")
        return run_id
    except Exception as e:
        log(f"ERROR retrieving RunGUID: {e}")
        sys.exit(1)

# === Random raw-file generator (for test mode) ===
def generate_random_raw_file(raw_path: str, header_pair: str) -> str:
    """
    Write a random raw file in the expected Gen5-like format:
    <header_pair>
    Well\t<header_pair> - Mean
    A1\t<value>
    ...
    """
    try:
        parent = os.path.dirname(raw_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        ws = wells_96()
        values = []
        for _ in ws:
            r = random.random()
            if r < 0.70:
                v = random.uniform(0, 50)
            elif r < 0.95:
                v = random.uniform(50, 500)
            else:
                v = random.uniform(500, 2200)
            # mix of '1234' and '56.' like your example
            if random.random() < 0.4:
                val_str = f"{int(round(v))}."
            else:
                val_str = str(int(round(v)))
            values.append(val_str)

        lines = [header_pair, f"Well\t{header_pair} - Mean"]
        for well, val in zip(ws, values):
            lines.append(f"{well}\t{val}")

        with open(raw_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

        log(f"Generated TEST raw file with random readings: {raw_path}")
        return raw_path
    except Exception as e:
        log(f"ERROR generating test raw file {raw_path}: {e}")
        sys.exit(1)

# === Parse Raw Data File ===
def parse_raw_data(raw_path, plate_id, run_id):
    try:
        if not os.path.isfile(raw_path):
            log(f"ERROR: Raw data file not found: {raw_path}")
            sys.exit(1)

        with open(raw_path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
        log(f"Read {len(lines)} lines from raw data file: {raw_path}")

        if len(lines) < 3:
            log("ERROR: Raw file appears too short or malformed (need headers + data).")
            sys.exit(1)

        data_lines = lines[2:]
        wells, values = [], []

        for line in data_lines:
            parts = line.replace("\t", " ").split()
            if len(parts) < 2:
                continue
            well = parts[0].strip()
            token = parts[-1].strip().rstrip('.')  # e.g., "5." -> "5"
            if token in ("", "-", "."):
                token = "0"
            try:
                value = float(token)
            except ValueError:
                filtered = ''.join(ch for ch in token if (ch.isdigit() or ch in ".-"))
                if filtered in ("", ".", "-"):
                    value = 0.0
                else:
                    try:
                        value = float(filtered)
                    except Exception:
                        value = 0.0

            wells.append(well)
            values.append(value)

        if not wells:
            log("ERROR: No well data parsed from raw file.")
            sys.exit(1)

        df = pd.DataFrame({
            "PlateID": plate_id,
            "WellID": wells,
            "Value": values,
            "RunID": run_id
        })
        log(f"Parsed {len(df)} wells from raw data file.")
        return df
    except Exception as e:
        log(f"ERROR parsing raw data file {raw_path}: {e}")
        sys.exit(1)

# === Write DF to BCP-Compatible File ===
def write_bcp_file(df, file_path):
    try:
        df.to_csv(file_path, sep="\t", header=False, index=False)
        log(f"BCP input file written: {file_path}")
    except Exception as e:
        log(f"ERROR writing BCP file {file_path}: {e}")
        sys.exit(1)

# === Execute BCP Silently ===
def run_bcp(table_name, file_path):
    bcp_cmd = [
        "bcp",
        f"EvoYeast.dbo.{table_name}",
        "in", file_path,
        "-T", "-c",
        "-S", "HAMILTON-PC\\HAMILTON"
    ]
    try:
        log(f"Executing BCP silently: {' '.join(bcp_cmd)}")
        result = subprocess.run(
            bcp_cmd,
            capture_output=True,
            text=True,
            creationflags=subprocess.CREATE_NO_WINDOW  # No window
        )
        if result.returncode != 0:
            log(f"BCP FAILED: {result.stderr}")
            sys.exit(1)
        log(f"BCP upload to {table_name} successful. Output: {result.stdout}")
    except Exception as e:
        log(f"ERROR executing BCP for table {table_name}: {e}")
        sys.exit(1)

# === Main Workflow ===
def main():
    try:
        # Step 1: Get RunID and Determine whether in test-mode
        run_id = get_runID()
        if TEST_MODE:
            log("Running Under test-mode")

        # Step 2: Retrieve PlateID
        conn = establish_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT PlateID FROM Plates WHERE BarCode = ?", (PlateBarcode,))
        row = cursor.fetchone()
        if not row:
            log(f"ERROR: No PlateID found for barcode {PlateBarcode}.")
            sys.exit(1)
        plate_id = row[0]
        log(f"Retrieved PlateID: {plate_id}")
        conn.close()

        # Step 2.5: Select raw paths; in test-mode, generate and use *_test files (preserve extension presence)
        raw482 = RawDataPath_FlEx482Em510
        raw587 = RawDataPath_FlEx587Em611

        if TEST_MODE:
            test482 = add_test_suffix_preserve_ext(raw482)
            test587 = add_test_suffix_preserve_ext(raw587)
            generate_random_raw_file(test482, "482,510")
            generate_random_raw_file(test587, "587,611")
            raw482, raw587 = test482, test587

        # Step 3: Parse raw fluorescence data (both wavelengths)
        df_482 = parse_raw_data(raw482, plate_id, run_id)
        df_587 = parse_raw_data(raw587, plate_id, run_id)

        # Step 4: Define temp file paths for BCP payloads
        task_dir = r"C:\EvoTaskFiles"
        os.makedirs(task_dir, exist_ok=True)
        bcp_482 = os.path.join(task_dir, f"{run_id}_FlEx482Em510.txt")
        bcp_587 = os.path.join(task_dir, f"{run_id}_FlEx587Em611.txt")

        # Step 5: Write data to BCP files
        write_bcp_file(df_482, bcp_482)
        write_bcp_file(df_587, bcp_587)

        # Step 6: Perform BCP upload
        run_bcp("ImportFlEx482Em510", bcp_482)
        run_bcp("ImportFlEx587Em611", bcp_587)

        log("=== Fluorescence data upload completed successfully ===")
        sys.exit(0)

    except Exception as e:
        log(f"Fatal error: {e}")
        sys.exit(1)

# === Entry Point ===
if __name__ == "__main__":
    main()
