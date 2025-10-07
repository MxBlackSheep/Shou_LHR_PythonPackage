import pyodbc
import os
import sys
import pandas as pd
import random
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
args = parser.parse_args()
PlateBarcode = args.PlateBarcode

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

# === Generate Random Fluorescence Data ===
def generate_two_dfs(plate_id, run_id):
    rows = ["A", "B", "C", "D", "E", "F", "G", "H"]
    cols = range(1, 13)
    well_ids = [f"{r}{c}" for c in cols for r in rows]

    def rand_vol():
        return round(random.uniform(0, 550), 6)

    df1 = pd.DataFrame({
        "PlateID": plate_id,
        "WellID": well_ids,
        "FlEx482Em510": [rand_vol() for _ in well_ids],
        "RunID": run_id
    })
    df2 = pd.DataFrame({
        "PlateID": plate_id,
        "WellID": well_ids,
        "FlEx587Em611": [rand_vol() for _ in well_ids],
        "RunID": run_id
    })
    log(f"Generated random fluorescence data for {len(well_ids)} wells.")
    return df1, df2

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
            creationflags=subprocess.CREATE_NO_WINDOW  # ✅ Hide CMD window
        )
        if result.returncode != 0:
            log(f"BCP FAILED: {result.stderr}")
            sys.exit(1)
        log(f"BCP upload to {table_name} successful. Output: {result.stdout}")
    except Exception as e:
        log(f"ERROR executing BCP for table {table_name}: {e}")
        sys.exit(1)

# === Delete Temporary BCP File ===
def delete_file(file_path):
    try:
        os.remove(file_path)
        log(f"Deleted temporary file: {file_path}")
    except Exception as e:
        log(f"WARNING: Failed to delete {file_path}: {e}")

# === Main Workflow ===
def main():
    try:
        # Step 1: Get RunID
        run_id = get_runID()

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

        # Step 3: Generate fluorescence data
        EM510, EM611 = generate_two_dfs(plate_id, run_id)

        # Step 4: Define temp file paths
        task_dir = r"C:\EvoTaskFiles"
        os.makedirs(task_dir, exist_ok=True)
        file_510 = os.path.join(task_dir, f"{run_id}_FlEx482Em510.txt")
        file_611 = os.path.join(task_dir, f"{run_id}_FlEx587Em611.txt")

        # Step 5: Write data to BCP files
        write_bcp_file(EM510, file_510)
        write_bcp_file(EM611, file_611)

        # Step 6: Perform BCP upload
        run_bcp("ImportFlEx482Em510", file_510)
        run_bcp("ImportFlEx587Em611", file_611)

        # Step 7: Delete temporary files
        delete_file(file_510)
        delete_file(file_611)

        log("=== Fluorescence data upload completed successfully ===")
        sys.exit(0)

    except Exception as e:
        log(f"Fatal error: {e}")
        sys.exit(1)

# === Entry Point ===
if __name__ == "__main__":
    main()
