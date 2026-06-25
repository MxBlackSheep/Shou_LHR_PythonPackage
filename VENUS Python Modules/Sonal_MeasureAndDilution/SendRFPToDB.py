import pyodbc
import os
import sys
import random
import pandas as pd
import argparse
from datetime import datetime


# ============================================================
# Logging Setup
# ============================================================

log_dir = r"C:\Python Log"
os.makedirs(log_dir, exist_ok=True)

script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(log_dir, f"{script_name}_{timestamp}.log")


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {msg}\n")


# ============================================================
# Safe argparse
# ============================================================

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


parser = SafeArgumentParser(
    description="Parse RFP fluorescence data and directly insert into ChampionsCulturesHistory."
)
parser.add_argument("PlateBarcode", type=str, help="Plate Barcode Identifier")
args = parser.parse_args()

PlateBarcode = args.PlateBarcode


# ============================================================
# Constants
# ============================================================

RawDataPath_FlEx587Em611 = r"C:\Program Files\HAMILTON\Gen5Data\FlEx587Em611_data"

BASELINE_RFP = 10.0
RFP_OD_SCALE = 2262.3
CULTURE_LABEL = "Cells"

MISSING_GFP_VALUE = -1.0
MISSING_GFP_OD_VALUE = -1.0

DEFAULT_INITIAL_OD = 0.01


# ============================================================
# Helpers
# ============================================================

def add_test_suffix_preserve_ext(path: str) -> str:
    root, ext = os.path.splitext(path)
    return f"{root}_test{ext}"


def wells_96():
    rows = ["A", "B", "C", "D", "E", "F", "G", "H"]
    cols = range(1, 13)
    return [f"{r}{c}" for c in cols for r in rows]


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


# ============================================================
# Database Lookup Functions
# ============================================================

def get_runID():
    conn = None

    try:
        conn = establish_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT TOP 1 RunGUID
            FROM HamiltonVectorDB.dbo.HxRun
            ORDER BY StartTime DESC
            """
        )

        row = cursor.fetchone()

        if not row:
            raise RuntimeError("No RunGUID found in HamiltonVectorDB.dbo.HxRun.")

        run_id = row[0]
        log(f"Retrieved RunGUID: {run_id}")

        conn.close()
        return run_id

    except Exception as e:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

        log(f"ERROR retrieving RunGUID: {e}")
        sys.exit(1)


def get_active_experiment_and_test_mode():
    conn = None

    try:
        conn = establish_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT ExperimentID, UserDefinedID
            FROM Experiments
            WHERE ScheduledToRun = 1
            """
        )

        rows = cursor.fetchall()

        if len(rows) == 0:
            raise RuntimeError("No active experiment found where ScheduledToRun = 1.")

        if len(rows) > 1:
            active_list = ", ".join(
                [f"{row.ExperimentID}:{row.UserDefinedID}" for row in rows]
            )
            raise RuntimeError(
                f"Multiple active experiments found where ScheduledToRun = 1: {active_list}"
            )

        experiment_id = rows[0].ExperimentID
        user_defined_id = rows[0].UserDefinedID

        log(
            f"Active experiment found: "
            f"ExperimentID={experiment_id}, UserDefinedID={user_defined_id}"
        )

        cursor.execute(
            """
            SELECT ParamValueTxt
            FROM ExperimentParameters
            WHERE ExperimentID = ?
              AND ParameterName = 'is_test'
            """,
            (experiment_id,)
        )

        row = cursor.fetchone()

        if row is None:
            log("WARNING: is_test not found. Defaulting to is_test = 0.")
            is_test = 0
        else:
            raw_value = str(row.ParamValueTxt).strip()
            try:
                is_test = int(float(raw_value))
            except ValueError:
                raise RuntimeError(
                    f"Invalid is_test value for ExperimentID={experiment_id}: {raw_value}"
                )

        log(f"Resolved is_test={is_test} for ExperimentID={experiment_id}")

        conn.close()
        return experiment_id, user_defined_id, is_test

    except Exception as e:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

        log(f"ERROR retrieving active experiment/test_Sonal_MeasureAndDilution mode: {e}")
        sys.exit(1)


def get_plate_id_from_barcode(plate_barcode):
    conn = None

    try:
        conn = establish_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT PlateID
            FROM Plates
            WHERE BarCode = ?
            """,
            (plate_barcode,)
        )

        row = cursor.fetchone()

        if not row:
            raise RuntimeError(f"No PlateID found for barcode {plate_barcode}.")

        plate_id = row[0]
        log(f"Retrieved PlateID={plate_id} for barcode={plate_barcode}")

        conn.close()
        return plate_id

    except Exception as e:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

        log(f"ERROR retrieving PlateID from barcode: {e}")
        sys.exit(1)


# ============================================================
# Test Raw File Generator
# ============================================================

def generate_random_raw_file(raw_path: str, header_pair: str) -> str:
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
        log(f"ERROR generating test_Sonal_MeasureAndDilution raw file {raw_path}: {e}")
        sys.exit(1)


# ============================================================
# Raw File Parser
# ============================================================

def parse_raw_data(raw_path, plate_id, run_id):
    try:
        if not os.path.isfile(raw_path):
            log(f"ERROR: Raw data file not found: {raw_path}")
            sys.exit(1)

        with open(raw_path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]

        log(f"Read {len(lines)} lines from raw data file: {raw_path}")

        if len(lines) < 3:
            log("ERROR: Raw file appears too short or malformed. Need headers plus data.")
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
                filtered = "".join(
                    ch for ch in token
                    if ch.isdigit() or ch in ".-"
                )

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
            "RunID": run_id,
        })

        log(f"Parsed {len(df)} wells from raw data file.")
        return df

    except Exception as e:
        log(f"ERROR parsing raw data file {raw_path}: {e}")
        sys.exit(1)


# ============================================================
# Main Database Operation
# ============================================================

def process_rfp_and_insert_champions_history(
    df_rfp: pd.DataFrame,
    plate_id: int,
    run_id: str,
    culture_label: str = CULTURE_LABEL,
    baseline_rfp: float = BASELINE_RFP,
    rfp_od_scale: float = RFP_OD_SCALE,
):
    conn = None

    try:
        conn = establish_connection()
        conn.autocommit = False
        cursor = conn.cursor()

        log("Starting RFP-only direct database update.")

        # ------------------------------------------------------------
        # 1. Get run timestamp
        # ------------------------------------------------------------
        cursor.execute(
            """
            SELECT StartTime
            FROM HamiltonVectorDB.dbo.HxRun
            WHERE RunGUID = ?
            """,
            (run_id,)
        )

        row = cursor.fetchone()

        if row is None or row[0] is None:
            log(f"WARNING: No StartTime found for RunGUID={run_id}. Using current datetime.")
            run_timestamp = datetime.now()
        else:
            run_timestamp = row[0]

        log(f"Resolved run timestamp: {run_timestamp}")

        # ------------------------------------------------------------
        # 2. Load plate pattern
        # ------------------------------------------------------------
        cursor.execute(
            """
            SELECT PlateID, WellID, WellAssign
            FROM ImportPlatePattern
            WHERE PlateID = ?
            """,
            (plate_id,)
        )

        pattern_rows = cursor.fetchall()

        if not pattern_rows:
            raise RuntimeError(
                f"No plate pattern found in ImportPlatePattern for PlateID={plate_id}."
            )

        df_pattern = pd.DataFrame.from_records(
            [(r.PlateID, r.WellID, r.WellAssign) for r in pattern_rows],
            columns=["PlateID", "WellID", "WellAssign"]
        )

        log(f"Loaded {len(df_pattern)} plate-pattern rows for PlateID={plate_id}.")

        # ------------------------------------------------------------
        # 3. Merge raw RFP with plate pattern
        # ------------------------------------------------------------
        df = df_rfp.copy()

        df["PlateID"] = df["PlateID"].astype(int)
        df["WellID"] = df["WellID"].astype(str).str.strip()
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce").fillna(0.0)

        df_pattern["PlateID"] = df_pattern["PlateID"].astype(int)
        df_pattern["WellID"] = df_pattern["WellID"].astype(str).str.strip()
        df_pattern["WellAssign"] = df_pattern["WellAssign"].astype(str).str.strip()

        merged = df.merge(
            df_pattern,
            on=["PlateID", "WellID"],
            how="inner"
        )

        if merged.empty:
            raise RuntimeError(
                f"No matching wells between parsed RFP data and ImportPlatePattern "
                f"for PlateID={plate_id}."
            )

        missing_pattern_wells = sorted(set(df["WellID"]) - set(merged["WellID"]))
        if missing_pattern_wells:
            log(
                "WARNING: Some wells from raw RFP file were not found in plate pattern: "
                f"{missing_pattern_wells}"
            )

        missing_rfp_wells = sorted(set(df_pattern["WellID"]) - set(merged["WellID"]))
        if missing_rfp_wells:
            log(
                "WARNING: Some wells from plate pattern were not found in raw RFP file: "
                f"{missing_rfp_wells}"
            )

        log(f"Merged {len(merged)} RFP rows with plate pattern.")

        # ------------------------------------------------------------
        # 4. RFP-only correction
        # ------------------------------------------------------------
        merged["FlEx587Em611"] = merged["Value"].clip(lower=baseline_rfp)
        merged["OD_FlEx587Em611"] = merged["FlEx587Em611"] / rfp_od_scale

        merged["FlEx482Em510"] = MISSING_GFP_VALUE
        merged["OD_FlEx482Em510"] = MISSING_GFP_OD_VALUE

        log(
            f"Applied RFP-only correction: baseline={baseline_rfp}, "
            f"RFP_OD_SCALE={rfp_od_scale}"
        )

        culture_rows = merged[merged["WellAssign"] == culture_label].copy()
        control_rows = merged[merged["WellAssign"] != culture_label].copy()

        log(f"Culture wells where WellAssign='{culture_label}': {len(culture_rows)}")
        log(f"Control wells where WellAssign<>'{culture_label}': {len(control_rows)}")

        if culture_rows.empty:
            raise RuntimeError(
                f"No culture wells found where WellAssign='{culture_label}'."
            )

        # ------------------------------------------------------------
        # 5. Check existing Cultures for this PlateID
        # ------------------------------------------------------------
        cursor.execute(
            """
            SELECT CultureID, WellID
            FROM dbo.Cultures
            WHERE PlateID = ?
            """,
            (plate_id,)
        )

        existing_culture_rows = cursor.fetchall()
        existing_culture_map = {
            str(row.WellID).strip(): row.CultureID
            for row in existing_culture_rows
        }

        log(f"Existing Cultures for PlateID={plate_id}: {len(existing_culture_map)}")

        culture_ids = {}

        # ------------------------------------------------------------
        # 6. Insert missing Cultures only
        # ------------------------------------------------------------
        inserted_cultures = 0

        for _, row in culture_rows.iterrows():
            well_id = row["WellID"]

            if well_id in existing_culture_map:
                culture_ids[well_id] = existing_culture_map[well_id]
                continue

            cursor.execute(
                """
                SELECT dbo.GenerateCultureID(?, ?)
                """,
                (plate_id, well_id)
            )

            culture_id_row = cursor.fetchone()

            if culture_id_row is None or culture_id_row[0] is None:
                raise RuntimeError(
                    f"GenerateCultureID returned NULL for PlateID={plate_id}, WellID={well_id}."
                )

            culture_id = culture_id_row[0]

            cursor.execute(
                """
                INSERT INTO dbo.Cultures (CultureID, PlateID, WellID)
                VALUES (?, ?, ?)
                """,
                (culture_id, plate_id, well_id)
            )

            culture_ids[well_id] = culture_id
            existing_culture_map[well_id] = culture_id
            inserted_cultures += 1

        log(f"Inserted {inserted_cultures} new rows into Cultures.")
        log(f"Total usable culture IDs for this run: {len(culture_ids)}")

        # ------------------------------------------------------------
        # 7. Insert Controls only if this plate does not already have them
        # ------------------------------------------------------------
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM dbo.Controls
            WHERE PlateID = ?
            """,
            (plate_id,)
        )

        existing_controls_count = cursor.fetchone()[0]

        inserted_controls = 0

        if existing_controls_count > 0:
            log(
                f"Controls already exist for PlateID={plate_id}; "
                f"skipping Controls insertion."
            )
        else:
            for _, row in control_rows.iterrows():
                cursor.execute(
                    """
                    INSERT INTO dbo.Controls
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        plate_id,
                        row["WellID"],
                        1,
                        -1
                    )
                )
                inserted_controls += 1

            log(f"Inserted {inserted_controls} rows into Controls.")

        # ------------------------------------------------------------
        # 8. Insert Propagation only for newly inserted cultures
        # ------------------------------------------------------------
        inserted_propagation = 0

        if inserted_cultures == 0:
            log(
                f"No new Cultures inserted for PlateID={plate_id}; "
                f"skipping Propagation insertion."
            )
        else:
            for well_id, culture_id in culture_ids.items():
                if well_id not in [r["WellID"] for _, r in culture_rows.iterrows()]:
                    continue

                if well_id in existing_culture_map and inserted_cultures == 0:
                    continue

            # Simpler and safer: insert propagation only for cultures that did not exist before.
            for _, row in culture_rows.iterrows():
                well_id = row["WellID"]
                culture_id = culture_ids[well_id]

                # If this culture existed before this run, do not create a new root propagation row.
                if well_id in {
                    str(r.WellID).strip()
                    for r in existing_culture_rows
                }:
                    continue

                cursor.execute(
                    """
                    INSERT INTO dbo.Propagation
                    VALUES (?, ?)
                    """,
                    (
                        None,
                        culture_id
                    )
                )
                inserted_propagation += 1

            log(f"Inserted {inserted_propagation} rows into Propagation.")

        # ------------------------------------------------------------
        # 9. Determine next Iteration for each CultureID
        # ------------------------------------------------------------
        culture_id_values = list(culture_ids.values())

        latest_iteration = {}

        if culture_id_values:
            placeholders = ",".join(["?"] * len(culture_id_values))

            cursor.execute(
                f"""
                SELECT CultureID, MAX(Iteration) AS MaxIteration
                FROM dbo.ChampionsCulturesHistory
                WHERE CultureID IN ({placeholders})
                GROUP BY CultureID
                """,
                culture_id_values
            )

            latest_iteration = {
                row.CultureID: row.MaxIteration
                for row in cursor.fetchall()
            }

        # ------------------------------------------------------------
        # 10. Insert into ChampionsCulturesHistory
        # ------------------------------------------------------------
        history_rows = []

        for _, row in culture_rows.iterrows():
            well_id = row["WellID"]
            culture_id = culture_ids[well_id]

            previous_iteration = latest_iteration.get(culture_id, 0) or 0
            next_iteration = int(previous_iteration) + 1

            # For RFP-only workflow, use RFP-derived OD as the OD value.
            od_value = float(row["OD_FlEx587Em611"])

            history_rows.append(
                (
                    int(culture_id),
                    next_iteration,
                    od_value,
                    run_timestamp,
                    float(row["FlEx482Em510"]),
                    float(row["FlEx587Em611"]),
                    float(row["OD_FlEx482Em510"]),
                    float(row["OD_FlEx587Em611"]),
                )
            )

        if not history_rows:
            raise RuntimeError("No ChampionsCulturesHistory rows prepared for insertion.")

        insert_history_sql = """
            INSERT INTO dbo.ChampionsCulturesHistory
            (
                CultureID,
                Iteration,
                OD,
                [TimeStamp],
                FlEx482Em510,
                FlEx587Em611,
                OD_FlEx482Em510,
                OD_FlEx587Em611
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor.fast_executemany = True
        cursor.executemany(insert_history_sql, history_rows)

        log(f"Inserted {len(history_rows)} rows into ChampionsCulturesHistory.")

        # ------------------------------------------------------------
        # 11. Commit
        # ------------------------------------------------------------
        conn.commit()
        conn.close()

        log("RFP-only direct database update completed successfully.")

    except Exception as e:
        if conn is not None:
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass

        log(f"ERROR during RFP-only direct database update: {e}")
        sys.exit(1)


# ============================================================
# Main
# ============================================================

def main():
    try:
        log("=== Starting RFP-only ChampionsCulturesHistory script ===")
        log(f"Input PlateBarcode={PlateBarcode}")

        run_id = get_runID()

        experiment_id, user_defined_id, is_test = get_active_experiment_and_test_mode()
        test_mode = is_test == 1

        if test_mode:
            log("Running under TEST MODE from ExperimentParameters.is_test = 1")
        else:
            log("Running under normal mode from ExperimentParameters.is_test = 0")

        plate_id = get_plate_id_from_barcode(PlateBarcode)

        raw587 = RawDataPath_FlEx587Em611

        if test_mode:
            test587 = add_test_suffix_preserve_ext(raw587)
            generate_random_raw_file(test587, "587,611")
            raw587 = test587

        df_587 = parse_raw_data(raw587, plate_id, run_id)

        process_rfp_and_insert_champions_history(
            df_rfp=df_587,
            plate_id=plate_id,
            run_id=run_id,
            culture_label=CULTURE_LABEL,
            baseline_rfp=BASELINE_RFP,
            rfp_od_scale=RFP_OD_SCALE,
        )

        log("=== RFP-only ChampionsCulturesHistory script completed successfully ===")
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