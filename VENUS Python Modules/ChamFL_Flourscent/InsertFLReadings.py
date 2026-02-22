import pyodbc
import os
import sys
import random
import pandas as pd
import argparse
from datetime import datetime, timedelta

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


# === Safe argparse that never writes to stderr ===
class SafeArgumentParser(argparse.ArgumentParser):
    def _print_message(self, message, file=None):
        if message:
            try:
                log(message.strip())
            except Exception:
                pass

    def error(self, message):
        try:
            log(f"ARGPARSE ERROR: {message}")
        except Exception:
            pass
        raise SystemExit(2)


# === Parse Arguments (same style as original) ===
parser = SafeArgumentParser()
parser.add_argument("PlateBarcode", type=str, help="Plate Barcode Identifier")
parser.add_argument(
    "--test-mode",
    action="store_true",
    help="If set, generate fake fluorescence that follows the iteration→OD pattern.",
)
args = parser.parse_args()
PlateBarcode = args.PlateBarcode
TEST_MODE = args.test_mode

# Gen5 raw data paths (often no extension)
RawDataPath_FlEx482Em510 = r"C:\Program Files\HAMILTON\Gen5Data\FlEx482Em510_data"
RawDataPath_FlEx587Em611 = r"C:\Program Files\HAMILTON\Gen5Data\FlEx587Em611_data"


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
            token = parts[-1].strip().rstrip(".")
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


def build_test_fluorescence_dfs_from_iteration(
    plate_id: int,
    baseline_482: float = 10.0,
    baseline_587: float = 10.0,
):
    """
    Test mode only:
      Iteration 1..4 => OD = 0.1, 0.2, 0.3, 0.4
    OD is defined as OD_482 + OD_587.
    Split: OD_482 = OD/2, OD_587 = OD/2.
    """
    A = 4669.1
    D1 = 773.1
    B = 2262.3
    D2 = 305.0

    denom = A * B + D1 * D2
    det = A * B - D1 * D2
    if det == 0:
        raise RuntimeError("Inverse model determinant is 0; cannot solve for fluorescence.")

    def solve_fluorescence_for_od(od_482_target: float, od_587_target: float):
        rhs1 = denom * od_482_target
        rhs2 = denom * od_587_target
        fl482 = (A * rhs1 + D1 * rhs2) / det
        fl587 = (B * rhs2 + D2 * rhs1) / det
        return fl482, fl587

    od_by_iteration = {1: 0.1, 2: 0.2, 3: 0.3, 4: 0.4}

    conn = establish_connection()
    cur = conn.cursor()

    cur.execute("SELECT CultureID, WellID FROM dbo.Cultures WHERE PlateID = ?", (plate_id,))
    cultures = cur.fetchall()
    if not cultures:
        conn.close()
        raise RuntimeError(f"No cultures found for PlateID={plate_id}")

    cur.execute(
        """
        SELECT CultureID, MAX(Iteration) AS MaxIter
        FROM dbo.ChampionsCulturesHistory
        WHERE CultureID IN (SELECT CultureID FROM dbo.Cultures WHERE PlateID = ?)
        GROUP BY CultureID
        """,
        (plate_id,),
    )
    latest = dict(cur.fetchall())
    conn.close()

    wells_ = []
    fl482_vals = []
    fl587_vals = []

    for culture_id, well in cultures:
        next_iter = (latest.get(culture_id, 0) or 0) + 1

        if next_iter in od_by_iteration:
            od_target = od_by_iteration[next_iter]
            fl482, fl587 = solve_fluorescence_for_od(od_target / 2.0, od_target / 2.0)
            fl482 = max(float(fl482), baseline_482)
            fl587 = max(float(fl587), baseline_587)
        else:
            fl482 = random.uniform(baseline_482, baseline_482 + 2000)
            fl587 = random.uniform(baseline_587, baseline_587 + 2000)

        wells_.append(well)
        fl482_vals.append(fl482)
        fl587_vals.append(fl587)

    df_482 = pd.DataFrame({"WellID": wells_, "Value": fl482_vals})
    df_587 = pd.DataFrame({"WellID": wells_, "Value": fl587_vals})
    log(f"TEST_MODE: built fake fluorescence for {len(df_482)} wells.")
    return df_482, df_587


def insert_champions_cultureshistory_from_raw(
    plate_id: int,
    run_id: str,
    df_482: pd.DataFrame,
    df_587: pd.DataFrame,
    test_mode: bool,
    baseline_482: float = 10.0,
    baseline_587: float = 10.0,
):
    FlEx482Em510ODscale = 4669.1
    FlEx482Em510D_FlEx587Em611damp = 773.1
    FlEx587Em611ODscale = 2262.3
    FlEx587Em611D_FlEx482Em510damp = 305.0

    od_by_iteration = {1: 0.1, 2: 0.2, 3: 0.3, 4: 0.4}

    conn = establish_connection()
    cur = conn.cursor()

    # Default timestamp from the Run
    cur.execute("SELECT StartTime FROM HamiltonVectorDB.dbo.HxRun WHERE RunGUID = ?", (run_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise RuntimeError(f"No HxRun found for RunGUID={run_id}")
    ts_default = row[0]

    m482 = df_482[["WellID", "Value"]].rename(columns={"Value": "Fl482"})
    m587 = df_587[["WellID", "Value"]].rename(columns={"Value": "Fl587"})
    df = pd.merge(m482, m587, on="WellID", how="inner")

    df["Fl482_corr"] = df["Fl482"].clip(lower=baseline_482)
    df["Fl587_corr"] = df["Fl587"].clip(lower=baseline_587)

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

    cur.execute("SELECT CultureID, WellID FROM dbo.Cultures WHERE PlateID = ?", (plate_id,))
    mapping = {well: cid for (cid, well) in cur.fetchall()}

    df["CultureID"] = df["WellID"].map(mapping)
    df = df.dropna(subset=["CultureID"]).copy()
    if df.empty:
        conn.close()
        raise RuntimeError("No matching CultureID found for any well on this plate.")
    df["CultureID"] = df["CultureID"].astype(int)

    cur.execute(
        """
        SELECT CultureID, MAX(Iteration) AS MaxIter
        FROM dbo.ChampionsCulturesHistory
        WHERE CultureID IN (SELECT CultureID FROM dbo.Cultures WHERE PlateID = ?)
        GROUP BY CultureID
        """,
        (plate_id,),
    )
    latest = dict(cur.fetchall())
    df["Iteration"] = df["CultureID"].map(lambda cid: (latest.get(cid, 0) or 0) + 1)

    if test_mode:
        df["OD"] = df["Iteration"].map(lambda it: od_by_iteration.get(int(it), None))

        # Per-iteration timestamp offsets (only for iter 1..4)
        now = datetime.now()
        offset_hours = {1: 4, 2: 3, 3: 2, 4: 1}

        def ts_for_iter(it: int):
            h = offset_hours.get(int(it))
            if h is None:
                return ts_default
            return now - timedelta(hours=h)

        df["TS"] = df["Iteration"].map(ts_for_iter)
    else:
        df["OD"] = None
        df["TS"] = ts_default

    # Make a Python list of timestamps aligned with rows
    ts_list = df["TS"].tolist()

    rows = list(
        zip(
            df["CultureID"].tolist(),
            df["Iteration"].tolist(),
            df["OD"].tolist(),
            ts_list,
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


def main():
    try:
        run_id = get_runID()
        if TEST_MODE:
            log("Running under test mode")

        conn = establish_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT PlateID FROM dbo.Plates WHERE BarCode = ?", (PlateBarcode,))
        row = cursor.fetchone()
        if not row:
            log(f"ERROR: No PlateID found for barcode {PlateBarcode}.")
            conn.close()
            sys.exit(1)
        plate_id = row[0]
        log(f"Retrieved PlateID: {plate_id}")
        conn.close()

        if TEST_MODE:
            df_482, df_587 = build_test_fluorescence_dfs_from_iteration(plate_id=plate_id)
        else:
            df_482 = parse_raw_data(RawDataPath_FlEx482Em510, plate_id, run_id)
            df_587 = parse_raw_data(RawDataPath_FlEx587Em611, plate_id, run_id)

        insert_champions_cultureshistory_from_raw(
            plate_id=plate_id,
            run_id=run_id,
            df_482=df_482,
            df_587=df_587,
            test_mode=TEST_MODE,
        )

        log("ChampionsCulturesHistory insert completed successfully")
        sys.exit(0)

    except SystemExit as e:
        if isinstance(e.code, int) and e.code != 0:
            log(f"Exit with code {e.code}")
        raise
    except Exception as e:
        log(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()