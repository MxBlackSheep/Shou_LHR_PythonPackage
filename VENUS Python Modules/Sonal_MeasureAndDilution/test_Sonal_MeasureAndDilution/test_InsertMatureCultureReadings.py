import argparse
import os
import random
import sys
from datetime import datetime
from typing import Any

import pyodbc


# ============================================================
# Logging setup
# ============================================================

LOG_DIR = r"C:\Python Log"
os.makedirs(LOG_DIR, exist_ok=True)

SCRIPT_NAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_DIR, f"{SCRIPT_NAME}_{TIMESTAMP}.log")


def log(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {message}\n")


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


# ============================================================
# Constants
# ============================================================

TARGET_MIN_AVERAGE_OD = 0.3
RANDOM_OD_MIN = 0.20
RANDOM_OD_MAX = 0.45
AVERAGE_SAFETY_MARGIN = 0.001

BASELINE_RFP = 10.0
RFP_OD_SCALE = 2262.3
CULTURE_LABEL = "Cells"

# For this RFP-only test, GFP is treated as absent. Use 0 rather than -1 so
# OD_FlEx482Em510 + OD_FlEx587Em611 remains equal to the total OD.
GFP_VALUE = 0.0
GFP_OD_VALUE = 0.0


# ============================================================
# CLI
# ============================================================

parser = SafeArgumentParser(
    description=(
        "Testing helper: for a given plate barcode, insert random culture OD records "
        "into ChampionsCulturesHistory while ensuring average OD >= 0.3."
    )
)
parser.add_argument("PlateBarcode", type=str, help="Plate barcode to generate test OD records for")
args = parser.parse_args()
PlateBarcode = args.PlateBarcode


# ============================================================
# Database helpers
# ============================================================


def establish_connection() -> pyodbc.Connection:
    conn_str = (
        "DRIVER={ODBC Driver 11 for SQL Server};"
        "SERVER=LOCALHOST\\HAMILTON;"
        "DATABASE=EvoYeast;"
        "UID=Hamilton;"
        "PWD=mkdpw:V43;"
        "Trust_Connection=no;"
    )
    return pyodbc.connect(conn_str)


def fetch_one(cursor: pyodbc.Cursor, sql: str, params: tuple[Any, ...] = ()) -> Any:
    cursor.execute(sql, params)
    row = cursor.fetchone()
    if row is None:
        return None
    return row[0]


def get_latest_run_id(cursor: pyodbc.Cursor) -> str:
    run_id = fetch_one(
        cursor,
        """
        SELECT TOP 1 RunGUID
        FROM HamiltonVectorDB.dbo.HxRun
        ORDER BY StartTime DESC
        """,
    )
    if run_id is None:
        raise RuntimeError("No RunGUID found in HamiltonVectorDB.dbo.HxRun.")
    log(f"Retrieved latest RunGUID: {run_id}")
    return str(run_id)


def get_run_timestamp(cursor: pyodbc.Cursor, run_id: str) -> datetime:
    run_timestamp = fetch_one(
        cursor,
        """
        SELECT StartTime
        FROM HamiltonVectorDB.dbo.HxRun
        WHERE RunGUID = ?
        """,
        (run_id,),
    )
    if run_timestamp is None:
        log(f"WARNING: No StartTime found for RunGUID={run_id}. Using current datetime.")
        return datetime.now()
    return run_timestamp


def get_plate_id_from_barcode(cursor: pyodbc.Cursor, barcode: str) -> int:
    plate_id = fetch_one(
        cursor,
        """
        SELECT PlateID
        FROM dbo.Plates
        WHERE BarCode = ?
        """,
        (barcode,),
    )
    if plate_id is None:
        raise RuntimeError(f"No PlateID found for barcode={barcode}.")
    log(f"Retrieved PlateID={plate_id} for barcode={barcode}")
    return int(plate_id)


def load_plate_pattern(cursor: pyodbc.Cursor, plate_id: int):
    cursor.execute(
        """
        SELECT PlateID, WellID, WellAssign
        FROM dbo.ImportPlatePattern
        WHERE PlateID = ?
        ORDER BY dbo.ColOf96WellAddress(WellID), dbo.RowOf96WellAddress(WellID)
        """,
        (plate_id,),
    )
    rows = cursor.fetchall()
    if not rows:
        raise RuntimeError(f"No ImportPlatePattern rows found for PlateID={plate_id}.")
    log(f"Loaded {len(rows)} ImportPlatePattern rows for PlateID={plate_id}.")
    return rows


def generate_culture_id(cursor: pyodbc.Cursor, plate_id: int, well_id: str) -> int:
    culture_id = fetch_one(cursor, "SELECT dbo.GenerateCultureID(?, ?)", (plate_id, well_id))
    if culture_id is None:
        raise RuntimeError(f"GenerateCultureID returned NULL for PlateID={plate_id}, WellID={well_id}.")
    return int(culture_id)


def get_existing_culture_map(cursor: pyodbc.Cursor, plate_id: int) -> dict[str, int]:
    cursor.execute(
        """
        SELECT CultureID, WellID
        FROM dbo.Cultures
        WHERE PlateID = ?
        """,
        (plate_id,),
    )
    culture_map: dict[str, int] = {}
    for row in cursor.fetchall():
        if row.WellID is not None:
            culture_map[str(row.WellID).strip()] = int(row.CultureID)
    log(f"Found {len(culture_map)} existing non-anchor cultures for PlateID={plate_id}.")
    return culture_map


def get_next_iteration(cursor: pyodbc.Cursor, culture_id: int) -> int:
    max_iteration = fetch_one(
        cursor,
        """
        SELECT MAX(Iteration)
        FROM dbo.ChampionsCulturesHistory
        WHERE CultureID = ?
        """,
        (culture_id,),
    )
    if max_iteration is None:
        return 1
    return int(max_iteration) + 1


def generate_random_ods(count: int) -> list[float]:
    """
    Generate random OD values while guaranteeing the average is >= 0.3.

    The values are first sampled uniformly from RANDOM_OD_MIN to RANDOM_OD_MAX.
    If their average is below TARGET_MIN_AVERAGE_OD, all values are shifted upward
    by the same amount plus a small safety margin.
    """
    if count <= 0:
        raise ValueError("count must be greater than zero")

    ods = [random.uniform(RANDOM_OD_MIN, RANDOM_OD_MAX) for _ in range(count)]
    avg_od = sum(ods) / len(ods)

    if avg_od < TARGET_MIN_AVERAGE_OD:
        shift = TARGET_MIN_AVERAGE_OD - avg_od + AVERAGE_SAFETY_MARGIN
        ods = [od + shift for od in ods]
        avg_od = sum(ods) / len(ods)
        log(
            f"Initial random average OD was below target; shifted all OD values upward by {shift}."
        )

    log(
        f"Generated random OD values: count={len(ods)}, "
        f"min={min(ods)}, max={max(ods)}, average={avg_od}"
    )

    if avg_od < TARGET_MIN_AVERAGE_OD:
        raise RuntimeError(
            f"Internal error: generated average OD={avg_od}, "
            f"which is below target {TARGET_MIN_AVERAGE_OD}."
        )

    return ods


# ============================================================
# Main operation
# ============================================================


def insert_test_od_records(cursor: pyodbc.Cursor, plate_id: int, run_timestamp: datetime) -> None:
    pattern_rows = load_plate_pattern(cursor, plate_id)
    existing_culture_map = get_existing_culture_map(cursor, plate_id)

    culture_pattern_rows = [
        row for row in pattern_rows
        if str(row.WellAssign).strip() == CULTURE_LABEL
    ]
    control_pattern_rows = [
        row for row in pattern_rows
        if str(row.WellAssign).strip() != CULTURE_LABEL
    ]

    if not culture_pattern_rows:
        raise RuntimeError(
            f"No culture wells found where ImportPlatePattern.WellAssign='{CULTURE_LABEL}'."
        )

    log(f"Culture wells: {len(culture_pattern_rows)}")
    log(f"Control wells: {len(control_pattern_rows)}")
    log(f"Target minimum average OD={TARGET_MIN_AVERAGE_OD}")
    log(f"Random OD range before adjustment: {RANDOM_OD_MIN} to {RANDOM_OD_MAX}")

    random_ods = generate_random_ods(len(culture_pattern_rows))

    culture_ids_by_well: dict[str, int] = {}
    inserted_cultures = 0

    for row in culture_pattern_rows:
        well_id = str(row.WellID).strip()

        if well_id in existing_culture_map:
            culture_ids_by_well[well_id] = existing_culture_map[well_id]
            continue

        culture_id = generate_culture_id(cursor, plate_id, well_id)
        cursor.execute(
            """
            INSERT INTO dbo.Cultures (CultureID, PlateID, WellID)
            VALUES (?, ?, ?)
            """,
            (culture_id, plate_id, well_id),
        )
        culture_ids_by_well[well_id] = culture_id
        existing_culture_map[well_id] = culture_id
        inserted_cultures += 1

    log(f"Inserted {inserted_cultures} missing culture rows into dbo.Cultures.")

    inserted_controls = 0
    existing_controls_count = fetch_one(
        cursor,
        """
        SELECT COUNT(*)
        FROM dbo.Controls
        WHERE PlateID = ?
        """,
        (plate_id,),
    )

    if existing_controls_count and int(existing_controls_count) > 0:
        log(f"Controls already exist for PlateID={plate_id}; skipping Controls insertion.")
    else:
        for row in control_pattern_rows:
            cursor.execute(
                """
                INSERT INTO dbo.Controls
                VALUES (?, ?, ?, ?)
                """,
                (plate_id, str(row.WellID).strip(), 1, -1),
            )
            inserted_controls += 1
        log(f"Inserted {inserted_controls} rows into dbo.Controls.")

    # Add root propagation rows only for newly created cultures.
    inserted_root_propagation = 0
    if inserted_cultures > 0:
        for well_id, culture_id in culture_ids_by_well.items():
            try:
                cursor.execute(
                    """
                    INSERT INTO dbo.Propagation
                    VALUES (?, ?)
                    """,
                    (None, culture_id),
                )
                inserted_root_propagation += 1
            except Exception as e:
                log(
                    "WARNING: Could not insert root propagation row. "
                    "This may be because the root row already exists or due to a schema difference. "
                    f"CultureID={culture_id}, error={e}"
                )

    log(f"Inserted {inserted_root_propagation} root rows into dbo.Propagation.")

    history_rows = []
    inserted_ods = []

    for row, od_value in zip(culture_pattern_rows, random_ods):
        well_id = str(row.WellID).strip()
        culture_id = culture_ids_by_well[well_id]
        next_iteration = get_next_iteration(cursor, culture_id)

        rfp_value = max(BASELINE_RFP, od_value * RFP_OD_SCALE)
        od_rfp = rfp_value / RFP_OD_SCALE

        history_rows.append(
            (
                culture_id,
                next_iteration,
                od_value,
                run_timestamp,
                GFP_VALUE,
                rfp_value,
                GFP_OD_VALUE,
                od_rfp,
            )
        )
        inserted_ods.append(od_value)

        log(
            f"Prepared test history row: WellID={well_id}, CultureID={culture_id}, "
            f"Iteration={next_iteration}, OD={od_value}, FlEx587Em611={rfp_value}"
        )

    inserted_average = sum(inserted_ods) / len(inserted_ods)
    if inserted_average < TARGET_MIN_AVERAGE_OD:
        raise RuntimeError(
            f"Generated average OD={inserted_average}, which is below target {TARGET_MIN_AVERAGE_OD}."
        )

    cursor.fast_executemany = True
    cursor.executemany(
        """
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
        """,
        history_rows,
    )

    log(f"Inserted {len(history_rows)} rows into dbo.ChampionsCulturesHistory.")
    log(
        f"Average inserted OD across culture wells = {inserted_average}; "
        f"target minimum = {TARGET_MIN_AVERAGE_OD}"
    )


# ============================================================
# Main
# ============================================================


def main() -> int:
    conn = None

    try:
        log("=== Starting random test OD generator script ===")
        log(f"Input PlateBarcode={PlateBarcode}")

        conn = establish_connection()
        conn.autocommit = False
        cursor = conn.cursor()
        log("Database connection established.")

        run_id = get_latest_run_id(cursor)
        run_timestamp = get_run_timestamp(cursor, run_id)
        plate_id = get_plate_id_from_barcode(cursor, PlateBarcode)

        insert_test_od_records(cursor, plate_id, run_timestamp)

        conn.commit()
        conn.close()

        log("Committed database transaction.")
        log("=== Random test OD generator script completed successfully ===")
        return 0

    except Exception as e:
        if conn is not None:
            try:
                conn.rollback()
                conn.close()
                log("Rolled back database transaction after error.")
            except Exception:
                pass
        log(f"FATAL ERROR: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
