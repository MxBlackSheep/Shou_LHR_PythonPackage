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


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {msg}\n")


# === Parse Arguments ===
parser = argparse.ArgumentParser()
parser.add_argument("PlateBarcode", type=str, help="Plate Barcode Identifier")
parser.add_argument(
    "--test-mode",
    action="store_true",
    help="If set, generate random raw files with '_test' suffix (preserving presence/absence of extension) and use them for parsing.",
)
args = parser.parse_args()
PlateBarcode = args.PlateBarcode
TEST_MODE = args.test_mode

# Gen5 raw data paths (often no extension)
RawDataPath_FlEx482Em510 = r"C:\Program Files\HAMILTON\Gen5Data\FlEx482Em510_data"
RawDataPath_FlEx587Em611 = r"C:\Program Files\HAMILTON\Gen5Data\FlEx587Em611_data"


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
    rows = ["A", "B", "C", "D", "E", "F", "G", "H"]
    cols = range(1, 13)
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
            token = parts[-1].strip().rstrip(".")  # e.g., "5." -> "5"
            if token in ("", "-", "."):
                token = "0"
            try:
                value = float(token)
            except ValueError:
                filtered = "".join(ch for ch in token if (ch.isdigit() or ch in ".-"))
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

        df = pd.DataFrame(
            {
                "PlateID": plate_id,
                "WellID": wells,
                "Value": values,
                "RunID": run_id,
            }
        )
        log(f"Parsed {len(df)} wells from raw data file.")
        return df
    except Exception as e:
        log(f"ERROR parsing raw data file {raw_path}: {e}")
        sys.exit(1)


def insert_champions_cultureshistory_from_raw(
    plate_id: int,
    run_id: str,
    df_482: pd.DataFrame,
    df_587: pd.DataFrame,
    baseline_482: float = 10.0,
    baseline_587: float = 10.0,
):
    # constants from Champions_CommencePropagationFl
    FlEx482Em510ODscale = 4669.1
    FlEx482Em510D_FlEx587Em611damp = 773.1
    FlEx587Em611ODscale = 2262.3
    FlEx587Em611D_FlEx482Em510damp = 305.0

    conn = establish_connection()
    cur = conn.cursor()

    # timestamp from the Run
    cur.execute("SELECT StartTime FROM HamiltonVectorDB.dbo.HxRun WHERE RunGUID = ?", (run_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise RuntimeError(f"No HxRun found for RunGUID={run_id}")
    ts = row[0]

    # Merge channels by WellID (keep only wells present in both)
    m482 = df_482[["WellID", "Value"]].rename(columns={"Value": "Fl482"})
    m587 = df_587[["WellID", "Value"]].rename(columns={"Value": "Fl587"})
    df = pd.merge(m482, m587, on="WellID", how="inner")

    # Apply baselines
    df["Fl482_corr"] = df["Fl482"].clip(lower=baseline_482)
    df["Fl587_corr"] = df["Fl587"].clip(lower=baseline_587)

    # Compute OD contributions exactly as in the proc
    denom = (
        FlEx482Em510ODscale * FlEx587Em611ODscale
        + FlEx482Em510D_FlEx587Em611damp * FlEx587Em611D_FlEx482Em510damp
    )

    df["OD_482"] = (
        (FlEx587Em611ODscale * df["Fl482_corr"] - FlEx482Em510D_FlEx587Em611damp * df["Fl587_corr"])
        / denom
    )

    df["OD_587"] = (
        (FlEx482Em510ODscale * df["Fl587_corr"] - FlEx587Em611D_FlEx482Em510damp * df["Fl482_corr"])
        / denom
    )

    # Look up CultureID for this plate/well
    cur.execute("SELECT CultureID, WellID FROM Cultures WHERE PlateID = ?", (plate_id,))
    mapping = {well: cid for (cid, well) in cur.fetchall()}

    df["CultureID"] = df["WellID"].map(mapping)
    df = df.dropna(subset=["CultureID"]).copy()
    if df.empty:
        conn.close()
        raise RuntimeError("No matching CultureID found for any well on this plate.")

    df["CultureID"] = df["CultureID"].astype(int)

    # Iteration starts at 1 per CultureID in ChampionsCulturesHistory
    cur.execute(
        """
        SELECT CultureID, MAX(Iteration) AS MaxIter
        FROM dbo.ChampionsCulturesHistory
        WHERE CultureID IN (SELECT CultureID FROM Cultures WHERE PlateID = ?)
        GROUP BY CultureID
        """,
        (plate_id,),
    )
    latest = dict(cur.fetchall())

    df["Iteration"] = df["CultureID"].map(lambda cid: (latest.get(cid, 0) or 0) + 1)

    # OD should be NULL for this script
    rows = list(
        zip(
            df["CultureID"].tolist(),
            df["Iteration"].tolist(),
            [None] * len(df),  # OD = NULL
            [ts] * len(df),
            df["Fl482_corr"].tolist(),
            df["Fl587_corr"].tolist(),
            df["OD_482"].tolist(),
            df["OD_587"].tolist(),
        )
    )

    sql = """
        INSERT INTO dbo.ChampionsCulturesHistory
        (CultureID, Iteration, OD, [TimeStamp],
         FlEx482Em510, FlEx587Em611, OD_FlEx482Em510, OD_FlEx587Em611)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    try:
        conn.autocommit = False
        cur.fast_executemany = True
        cur.executemany(sql, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# === Main Workflow ===
def main():
    try:
        run_id = get_runID()
        if TEST_MODE:
            log("Running under test-mode")

        # Retrieve PlateID by barcode
        conn = establish_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT PlateID FROM Plates WHERE BarCode = ?", (PlateBarcode,))
        row = cursor.fetchone()
        if not row:
            log(f"ERROR: No PlateID found for barcode {PlateBarcode}.")
            conn.close()
            sys.exit(1)
        plate_id = row[0]
        log(f"Retrieved PlateID: {plate_id}")
        conn.close()

        # Select raw paths; in test-mode, generate and use *_test files
        raw482 = RawDataPath_FlEx482Em510
        raw587 = RawDataPath_FlEx587Em611

        if TEST_MODE:
            test482 = add_test_suffix_preserve_ext(raw482)
            test587 = add_test_suffix_preserve_ext(raw587)
            generate_random_raw_file(test482, "482,510")
            generate_random_raw_file(test587, "587,611")
            raw482, raw587 = test482, test587

        # Parse raw fluorescence data
        df_482 = parse_raw_data(raw482, plate_id, run_id)
        df_587 = parse_raw_data(raw587, plate_id, run_id)

        # Insert into ChampionsCulturesHistory
        insert_champions_cultureshistory_from_raw(
            plate_id=plate_id,
            run_id=run_id,
            df_482=df_482,
            df_587=df_587,
        )

        log("=== ChampionsCulturesHistory insert completed successfully ===")
        sys.exit(0)

    except Exception as e:
        log(f"Fatal error: {e}")
        sys.exit(1)


# === Entry Point ===
if __name__ == "__main__":
    main()