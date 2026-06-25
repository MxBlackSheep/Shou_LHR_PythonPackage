import argparse
import os
import sys
from datetime import datetime
from typing import Any, Iterable, Optional

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

SERVER_NAME = r"LOCALHOST\HAMILTON"
DATABASE_NAME = "EvoYeast"
USERNAME = "Hamilton"
PASSWORD = "mkdpw:V43"
ODBC_DRIVER = "ODBC Driver 11 for SQL Server"

OUTPUT_DIR = r"C:\EvoTaskFiles"
LAYOUT_PATH = r"C:\PROGRAM FILES\HAMILTON\METHODS\LABPROTOCOLS\EXPERIMENTS\DECKS\SPATIALEVOLUTION3OD384WELL.LAY"

# Hard-coded Hamilton/VENUS labware IDs for sequence files
SOURCE_LABWARE_ID = "Corning_96_DW_1ml_P_DW_11_C_S_0004"
CHILD_LABWARE_ID = "Corning_96_DW_1ml_P_DW_11_C_S_0009"

HISTORY_TABLE = "dbo.ChampionsCulturesHistory"

TARGET_VOL = 300
INOCULATION_OD = 0.01
OD_SAMPLE_VOL = 0.0
INOCULATION_VOLUME_LIMIT = 0.0

CULTURE_LABEL = "Cells"
MEDIA_CTRL_LABEL = "MediaCtrl"


# ============================================================
# CLI
# ============================================================

parser = SafeArgumentParser(
    description=(
        "Create a child plate, propagate 100% of cultures from a parent plate, "
        "insert records into ChampionsCulturesHistory, and generate Hamilton sequence files. "
        "MediaCtrl wells are generated as media-only wells."
    )
)
parser.add_argument("ParentPlateBarcode", type=str, help="Existing parent/source plate barcode")
parser.add_argument("ChildPlateBarcode", type=str, help="New child/spillover plate barcode to create in Plates")
parser.add_argument("--layout-path", type=str, default=LAYOUT_PATH, help="Hamilton layout path used in sequence files")
parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR, help="Directory for generated EvoTaskFiles")
args = parser.parse_args()

PARENT_BARCODE = args.ParentPlateBarcode
CHILD_BARCODE = args.ChildPlateBarcode


# ============================================================
# Database helpers
# ============================================================

def establish_connection() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "Trust_Connection=no;"
    )
    return pyodbc.connect(conn_str)


def fetch_one_value(cursor: pyodbc.Cursor, sql: str, params: Iterable[Any] = ()) -> Any:
    cursor.execute(sql, tuple(params))
    row = cursor.fetchone()
    if row is None:
        return None
    return row[0]


def get_latest_run_id(cursor: pyodbc.Cursor) -> str:
    run_id = fetch_one_value(
        cursor,
        """
        SELECT TOP 1 RunGUID
        FROM HamiltonVectorDB.dbo.HxRun
        ORDER BY StartTime DESC
        """,
    )
    if not run_id:
        raise RuntimeError("No RunGUID found in HamiltonVectorDB.dbo.HxRun.")
    log(f"Retrieved latest RunGUID: {run_id}")
    return str(run_id)


def get_run_timestamp(cursor: pyodbc.Cursor, run_id: str) -> datetime:
    run_timestamp = fetch_one_value(
        cursor,
        """
        SELECT StartTime
        FROM HamiltonVectorDB.dbo.HxRun
        WHERE RunGUID = ?
        """,
        (run_id,),
    )
    if run_timestamp is None:
        log(f"WARNING: No StartTime found for RunGUID={run_id}; using current datetime.")
        return datetime.now()
    return run_timestamp


def get_plate_id_from_barcode(cursor: pyodbc.Cursor, barcode: str) -> Optional[int]:
    value = fetch_one_value(
        cursor,
        """
        SELECT PlateID
        FROM dbo.Plates
        WHERE BarCode = ?
        """,
        (barcode,),
    )
    return None if value is None else int(value)


def get_plate_metadata(cursor: pyodbc.Cursor, plate_id: int) -> dict[str, Any]:
    cursor.execute(
        """
        SELECT PlateID, Description, BarCode, CurrentIteration, Discarded
        FROM dbo.Plates
        WHERE PlateID = ?
        """,
        (plate_id,),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"PlateID={plate_id} not found in dbo.Plates.")

    return {
        "PlateID": int(row.PlateID),
        "Description": row.Description,
        "BarCode": row.BarCode,
        "CurrentIteration": row.CurrentIteration,
        "Discarded": row.Discarded,
    }


def create_child_plate(
    cursor: pyodbc.Cursor,
    child_barcode: str,
    parent_plate_id: int,
) -> int:
    existing_plate_id = get_plate_id_from_barcode(cursor, child_barcode)
    if existing_plate_id is not None:
        raise RuntimeError(
            f"Child plate barcode already exists in dbo.Plates: "
            f"BarCode={child_barcode}, PlateID={existing_plate_id}."
        )

    parent_plate = get_plate_metadata(cursor, parent_plate_id)

    parent_description = parent_plate.get("Description")
    child_description = "" if parent_description is None else str(parent_description)

    # Use child physical plate iteration = 1.
    child_iteration = 1

    cursor.execute(
        """
        INSERT INTO dbo.Plates (Description, BarCode, CurrentIteration, Discarded)
        VALUES (?, ?, ?, ?)
        """,
        (child_description, child_barcode, child_iteration, 0),
    )

    child_plate_id = get_plate_id_from_barcode(cursor, child_barcode)
    if child_plate_id is None:
        raise RuntimeError(f"Could not retrieve PlateID for new child barcode={child_barcode}")

    log(
        f"Created child plate: PlateID={child_plate_id}, "
        f"Description={child_description}, BarCode={child_barcode}, "
        f"CurrentIteration={child_iteration}, Discarded=0"
    )

    cursor.execute(
        """
        EXECUTE dbo.InsertPlateIntoCytomat ?
        """,
        (child_plate_id,),
    )
    log(f"Executed dbo.InsertPlateIntoCytomat for child PlateID={child_plate_id}.")

    return child_plate_id


def load_import_plate_pattern(cursor: pyodbc.Cursor, plate_id: int):
    cursor.execute(
        """
        SELECT PlateID, WellID, RunID, WellAssign
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


def copy_import_plate_pattern_to_child(
    cursor: pyodbc.Cursor,
    parent_plate_id: int,
    child_plate_id: int,
    run_id: str,
) -> None:
    parent_pattern_rows = load_import_plate_pattern(cursor, parent_plate_id)

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM dbo.ImportPlatePattern
        WHERE PlateID = ?
        """,
        (child_plate_id,),
    )
    existing_count = int(cursor.fetchone()[0])

    if existing_count > 0:
        log(
            f"ImportPlatePattern already has {existing_count} rows for child PlateID={child_plate_id}; "
            "skipping pattern copy."
        )
        return

    copied_rows = [
        (
            child_plate_id,
            str(row.WellID).strip(),
            run_id,
            str(row.WellAssign).strip(),
        )
        for row in parent_pattern_rows
    ]

    cursor.executemany(
        """
        INSERT INTO dbo.ImportPlatePattern (PlateID, WellID, RunID, WellAssign)
        VALUES (?, ?, ?, ?)
        """,
        copied_rows,
    )

    log(f"Copied {len(copied_rows)} ImportPlatePattern rows from parent to child plate.")


def load_parent_wells_for_child_plate(
    cursor: pyodbc.Cursor,
    parent_plate_id: int,
):
    """
    Load parent wells that should appear in the generated child-plate files.

    Cells:
        Require matching Cultures and latest ChampionsCulturesHistory records.
        These are propagated normally.

    MediaCtrl:
        Do not require Cultures/history records.
        These are generated as media-only rows:
            culture volume = 0
            media volume   = target volume
    """
    cursor.execute(
        f"""
        SELECT
            ipp.WellID,
            ipp.WellAssign,
            c.CultureID,
            h.Iteration,
            h.OD,
            h.FlEx482Em510,
            h.FlEx587Em611,
            h.OD_FlEx482Em510,
            h.OD_FlEx587Em611
        FROM dbo.ImportPlatePattern AS ipp
        LEFT JOIN dbo.Cultures AS c
            ON ipp.PlateID = c.PlateID
           AND ipp.WellID = c.WellID
        LEFT JOIN (
            SELECT h1.*
            FROM {HISTORY_TABLE} AS h1
            INNER JOIN (
                SELECT CultureID, MAX(Iteration) AS LatestIteration
                FROM {HISTORY_TABLE}
                GROUP BY CultureID
            ) AS latest
                ON h1.CultureID = latest.CultureID
               AND h1.Iteration = latest.LatestIteration
        ) AS h
            ON c.CultureID = h.CultureID
        WHERE ipp.PlateID = ?
          AND ipp.WellAssign IN (?, ?)
        ORDER BY dbo.ColOf96WellAddress(ipp.WellID), dbo.RowOf96WellAddress(ipp.WellID)
        """,
        (parent_plate_id, CULTURE_LABEL, MEDIA_CTRL_LABEL),
    )

    rows = cursor.fetchall()

    if not rows:
        raise RuntimeError(
            f"No {CULTURE_LABEL} or {MEDIA_CTRL_LABEL} wells found in ImportPlatePattern "
            f"for Parent PlateID={parent_plate_id}."
        )

    culture_count = 0
    media_ctrl_count = 0
    for row in rows:
        well_assign = str(row.WellAssign).strip()
        if well_assign == CULTURE_LABEL:
            culture_count += 1
        elif well_assign == MEDIA_CTRL_LABEL:
            media_ctrl_count += 1

    log(
        f"Loaded {len(rows)} parent wells from ImportPlatePattern: "
        f"{culture_count} {CULTURE_LABEL} wells, "
        f"{media_ctrl_count} {MEDIA_CTRL_LABEL} wells."
    )
    return rows


def get_cytomat_position(cursor: pyodbc.Cursor, plate_id: int) -> int:
    try:
        value = fetch_one_value(
            cursor,
            """
            SELECT CytomatPos
            FROM dbo.Cytomat
            WHERE PlateID = ?
            """,
            (plate_id,),
        )
        if value is None:
            return -1
        return int(value)
    except Exception as e:
        log(f"WARNING: Could not read Cytomat position for PlateID={plate_id}: {e}")
        return -1


def generate_culture_id(cursor: pyodbc.Cursor, plate_id: int, well_id: str) -> int:
    value = fetch_one_value(cursor, "SELECT dbo.GenerateCultureID(?, ?)", (plate_id, well_id))
    if value is None:
        raise RuntimeError(f"GenerateCultureID returned NULL for PlateID={plate_id}, WellID={well_id}.")
    return int(value)


def safe_float(value: Any, fallback: float = 0.0) -> float:
    if value is None:
        return fallback
    try:
        return float(value)
    except Exception:
        return fallback


def calculate_source_od(parent) -> float:
    """
    Prefer the main OD column.

    This matters for the RFP-only workflow, because OD_FlEx482Em510 may be
    stored as -1 in older records. If we blindly sum GFP OD + RFP OD, then:

        -1 + 0.3 = -0.7

    which is wrong. The main OD column is the safest value for propagation.
    """
    od = safe_float(parent.OD)
    if od > 0:
        return od

    od_482 = safe_float(parent.OD_FlEx482Em510)
    od_587 = safe_float(parent.OD_FlEx587Em611)

    # Ignore negative missing-channel sentinel values.
    if od_482 < 0:
        od_482 = 0.0
    if od_587 < 0:
        od_587 = 0.0

    return od_482 + od_587


# ============================================================
# Propagation logic
# ============================================================

def build_100_percent_propagation_rows(
    cursor: pyodbc.Cursor,
    parent_rows,
    child_plate_id: int,
    child_barcode: str,
    target_vol: float,
    inoculation_od: float,
    od_sample_vol: float,
    inoculation_volume_limit: float,
):
    cytomat_pos = get_cytomat_position(cursor, child_plate_id)
    effective_target_vol = target_vol

    if od_sample_vol:
        log(f"OD sample volume supplied: {od_sample_vol}")

    propagation_rows = []

    for parent in parent_rows:
        source_well = str(parent.WellID).strip()
        child_well = source_well
        well_assign = str(parent.WellAssign).strip()

        # ------------------------------------------------------------
        # Media-control wells
        # ------------------------------------------------------------
        # MediaCtrl wells are included in the Hamilton sequence/volume
        # files but are not treated as cultures.
        #
        # Required volumes:
        #   culture volume = 0
        #   media volume   = target volume
        # ------------------------------------------------------------
        if well_assign == MEDIA_CTRL_LABEL:
            propagation_rows.append(
                {
                    "WellAssign": well_assign,
                    "IsMediaCtrl": True,
                    "CytomatPos": cytomat_pos,
                    "SpilloverPlateBarcode": child_barcode,
                    "SpillOverPlateWellID": child_well,
                    "ToBePropagatedPlateWellID": source_well,
                    "InocVol": 0.0,
                    "MediaVol": float(effective_target_vol),
                    "ODAfterInoculation": 0.0,
                    "SpilloverPlateID": child_plate_id,
                    "ParentCultureID": None,
                    "ChildCultureID": None,
                    "FlEx482Em510AfterInoculation": 0.0,
                    "FlEx587Em611AfterInoculation": 0.0,
                    "OD_FlEx482Em510AfterInoculation": 0.0,
                    "OD_FlEx587Em611AfterInoculation": 0.0,
                    "InsufficientODUsedHalfVolume": False,
                }
            )

            log(
                f"Prepared {MEDIA_CTRL_LABEL} well: parent {source_well} -> child {child_well}; "
                f"CultureVol=0.0, MediaVol={effective_target_vol}"
            )
            continue

        # ------------------------------------------------------------
        # Normal culture wells
        # ------------------------------------------------------------
        if well_assign != CULTURE_LABEL:
            log(
                f"Skipping unsupported WellAssign='{well_assign}' "
                f"for WellID={source_well}."
            )
            continue

        if parent.CultureID is None:
            raise RuntimeError(
                f"Culture well has no CultureID: WellID={source_well}, "
                f"WellAssign={well_assign}."
            )

        source_od_total = calculate_source_od(parent)

        if source_od_total <= 0:
            raise RuntimeError(
                f"Source OD is zero or missing for CultureID={parent.CultureID}, "
                f"WellID={source_well}."
            )

        calculated_inoc_vol = inoculation_od * effective_target_vol / source_od_total
        inoc_vol = calculated_inoc_vol

        if inoculation_volume_limit and inoc_vol > inoculation_volume_limit:
            log(
                f"Inoculation volume limit applied for CultureID={parent.CultureID}, "
                f"WellID={source_well}: CalculatedInocVol={calculated_inoc_vol}, "
                f"Limit={inoculation_volume_limit}"
            )
            inoc_vol = inoculation_volume_limit

        # ------------------------------------------------------------
        # Insufficient OD handling
        # ------------------------------------------------------------
        # If the source OD is too low, the calculated inoculation volume
        # needed to reach InoculationOD would exceed the target well volume.
        #
        # Behaviour:
        #   still propagate, but use half target volume as culture and
        #   half target volume as media.
        # ------------------------------------------------------------
        insufficient_od_used_half_volume = False

        if inoc_vol > effective_target_vol:
            inoc_vol = effective_target_vol / 2.0
            insufficient_od_used_half_volume = True

            log(
                f"Insufficient OD for target inoculation but propagation is required. "
                f"Using half target volume as culture volume for CultureID={parent.CultureID}, "
                f"WellID={source_well}. "
                f"SourceOD={source_od_total}, "
                f"TargetOD={inoculation_od}, "
                f"CalculatedInocVol={calculated_inoc_vol}, "
                f"TargetVol={effective_target_vol}, "
                f"FinalInocVol={inoc_vol}, "
                f"FinalMediaVol={effective_target_vol - inoc_vol}"
            )

        media_vol = effective_target_vol - inoc_vol

        child_culture_id = generate_culture_id(cursor, child_plate_id, child_well)

        source_fl_482 = safe_float(parent.FlEx482Em510)
        source_fl_587 = safe_float(parent.FlEx587Em611)
        source_od_482 = safe_float(parent.OD_FlEx482Em510)
        source_od_587 = safe_float(parent.OD_FlEx587Em611)

        # Treat negative missing-channel values as 0 for the child record.
        if source_fl_482 < 0:
            source_fl_482 = 0.0
        if source_fl_587 < 0:
            source_fl_587 = 0.0
        if source_od_482 < 0:
            source_od_482 = 0.0
        if source_od_587 < 0:
            source_od_587 = 0.0

        od_after_inoculation = source_od_total * inoc_vol / effective_target_vol
        fl_482_after_inoculation = source_fl_482 * inoc_vol / effective_target_vol
        fl_587_after_inoculation = source_fl_587 * inoc_vol / effective_target_vol
        od_482_after_inoculation = source_od_482 * inoc_vol / effective_target_vol
        od_587_after_inoculation = source_od_587 * inoc_vol / effective_target_vol

        propagation_rows.append(
            {
                "WellAssign": well_assign,
                "IsMediaCtrl": False,
                "CytomatPos": cytomat_pos,
                "SpilloverPlateBarcode": child_barcode,
                "SpillOverPlateWellID": child_well,
                "ToBePropagatedPlateWellID": source_well,
                "InocVol": float(inoc_vol),
                "MediaVol": float(media_vol),
                "ODAfterInoculation": float(od_after_inoculation),
                "SpilloverPlateID": child_plate_id,
                "ParentCultureID": int(parent.CultureID),
                "ChildCultureID": child_culture_id,
                "FlEx482Em510AfterInoculation": float(fl_482_after_inoculation),
                "FlEx587Em611AfterInoculation": float(fl_587_after_inoculation),
                "OD_FlEx482Em510AfterInoculation": float(od_482_after_inoculation),
                "OD_FlEx587Em611AfterInoculation": float(od_587_after_inoculation),
                "InsufficientODUsedHalfVolume": insufficient_od_used_half_volume,
            }
        )

        log(
            f"Prepared propagation: parent {source_well} CultureID={parent.CultureID} "
            f"-> child {child_well} CultureID={child_culture_id}; "
            f"SourceOD={source_od_total}, "
            f"CalculatedInocVol={calculated_inoc_vol}, "
            f"FinalInocVol={inoc_vol}, "
            f"MediaVol={media_vol}, "
            f"ODAfterInoculation={od_after_inoculation}, "
            f"InsufficientODUsedHalfVolume={insufficient_od_used_half_volume}"
        )

    if not propagation_rows:
        raise RuntimeError("No culture or MediaCtrl rows prepared.")

    media_ctrl_count = sum(1 for row in propagation_rows if row.get("IsMediaCtrl", False))
    culture_count = len(propagation_rows) - media_ctrl_count

    log(
        f"Prepared {len(propagation_rows)} total rows: "
        f"{culture_count} culture rows, {media_ctrl_count} {MEDIA_CTRL_LABEL} rows."
    )
    return propagation_rows


def insert_child_records(cursor: pyodbc.Cursor, propagation_rows, run_timestamp: datetime) -> None:
    if not propagation_rows:
        raise RuntimeError("No propagation rows to insert.")

    culture_rows = [
        row for row in propagation_rows
        if not row.get("IsMediaCtrl", False)
    ]

    if not culture_rows:
        log(f"No culture rows to insert; only {MEDIA_CTRL_LABEL} rows found.")
        return

    cursor.executemany(
        """
        INSERT INTO dbo.Cultures (CultureID, PlateID, WellID)
        VALUES (?, ?, ?)
        """,
        [
            (
                row["ChildCultureID"],
                row["SpilloverPlateID"],
                row["SpillOverPlateWellID"],
            )
            for row in culture_rows
        ],
    )
    log(f"Inserted {len(culture_rows)} child culture rows into dbo.Cultures.")

    cursor.executemany(
        f"""
        INSERT INTO {HISTORY_TABLE}
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
        [
            (
                row["ChildCultureID"],
                1,
                row["ODAfterInoculation"],
                run_timestamp,
                row["FlEx482Em510AfterInoculation"],
                row["FlEx587Em611AfterInoculation"],
                row["OD_FlEx482Em510AfterInoculation"],
                row["OD_FlEx587Em611AfterInoculation"],
            )
            for row in culture_rows
        ],
    )
    log(f"Inserted {len(culture_rows)} child culture rows into {HISTORY_TABLE}.")

    cursor.executemany(
        """
        INSERT INTO dbo.Propagation
        VALUES (?, ?)
        """,
        [
            (
                row["ParentCultureID"],
                row["ChildCultureID"],
            )
            for row in culture_rows
        ],
    )
    log(f"Inserted {len(culture_rows)} parent-child rows into dbo.Propagation.")


# ============================================================
# Sequence file generation
# ============================================================

def write_plain_lines(path: str, values: list[str]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(values))
        if values:
            f.write("\n")
    log(f"File generated: {path}")


def write_hamilton_sequence_file(
    path: str,
    positions: list[str],
    labware: str,
    sequence_name: str,
    layout_path: str,
) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("Id,Layout,Sequence,Labware,Position\n")
        for i, position in enumerate(positions, 1):
            f.write(f"{i},{layout_path},{sequence_name},{labware},{position}\n")
    log(f"File generated: {path}")


def generate_sequence_files(
    run_id: str,
    propagation_rows,
    output_dir: str,
    layout_path: str,
    source_labware: str,
    child_labware: str,
) -> None:
    os.makedirs(output_dir, exist_ok=True)

    cytomat_pos = [str(row["CytomatPos"]) for row in propagation_rows]
    child_positions = [row["SpillOverPlateWellID"] for row in propagation_rows]
    source_positions = [row["ToBePropagatedPlateWellID"] for row in propagation_rows]
    culture_vol = [str(row["InocVol"]) for row in propagation_rows]
    media_vol = [str(row["MediaVol"]) for row in propagation_rows]

    cytomat_path = os.path.join(output_dir, f"{run_id}_CytomatPos.txt")
    culture_vol_path = os.path.join(output_dir, f"{run_id}_CultureVol.txt")
    media_vol_path = os.path.join(output_dir, f"{run_id}_MediaVol.txt")
    child_seq_path = os.path.join(output_dir, f"{run_id}_SpillOverPlate_Positions.txt")
    source_seq_path = os.path.join(output_dir, f"{run_id}_SpatialOverPlate_Positions.txt")

    write_plain_lines(cytomat_path, cytomat_pos)
    write_plain_lines(culture_vol_path, culture_vol)
    write_plain_lines(media_vol_path, media_vol)

    write_hamilton_sequence_file(
        child_seq_path,
        child_positions,
        child_labware,
        "seqSpillOverPlate",
        layout_path,
    )

    write_hamilton_sequence_file(
        source_seq_path,
        source_positions,
        source_labware,
        "seqEvoSrcPlate",
        layout_path,
    )


# ============================================================
# Main
# ============================================================

def main() -> int:
    conn = None

    try:
        log("=== Starting 100% child-plate propagation script ===")
        log(f"ParentPlateBarcode={PARENT_BARCODE}")
        log(f"ChildPlateBarcode={CHILD_BARCODE}")

        conn = establish_connection()
        conn.autocommit = False
        cursor = conn.cursor()
        log("Database connection established.")

        run_id = get_latest_run_id(cursor)
        run_timestamp = get_run_timestamp(cursor, run_id)
        log(f"Run timestamp: {run_timestamp}")

        target_vol = TARGET_VOL
        inoculation_od = INOCULATION_OD
        od_sample_vol = OD_SAMPLE_VOL
        inoculation_volume_limit = INOCULATION_VOLUME_LIMIT

        log(f"Using hard-coded TargetWellVolume={target_vol}")
        log(f"Using hard-coded InoculationOD={inoculation_od}")
        log(f"Using hard-coded OD_SAMPLE_VOL={od_sample_vol}")
        log(f"Using hard-coded INOCULATION_VOLUME_LIMIT={inoculation_volume_limit}")
        log("Forcing TopFractionToPropagate = 1.0 for 100% propagation.")
        log(f"Using hard-coded source labware ID={SOURCE_LABWARE_ID}")
        log(f"Using hard-coded child labware ID={CHILD_LABWARE_ID}")
        log(f"{MEDIA_CTRL_LABEL} wells will be generated with CultureVol=0 and MediaVol=TargetWellVolume.")

        parent_plate_id = get_plate_id_from_barcode(cursor, PARENT_BARCODE)
        if parent_plate_id is None:
            raise RuntimeError(f"Parent plate barcode not found in dbo.Plates: {PARENT_BARCODE}")
        log(f"Parent PlateID={parent_plate_id}")

        child_plate_id = create_child_plate(cursor, CHILD_BARCODE, parent_plate_id)

        copy_import_plate_pattern_to_child(
            cursor=cursor,
            parent_plate_id=parent_plate_id,
            child_plate_id=child_plate_id,
            run_id=run_id,
        )

        parent_rows = load_parent_wells_for_child_plate(
            cursor=cursor,
            parent_plate_id=parent_plate_id,
        )

        propagation_rows = build_100_percent_propagation_rows(
            cursor=cursor,
            parent_rows=parent_rows,
            child_plate_id=child_plate_id,
            child_barcode=CHILD_BARCODE,
            target_vol=target_vol,
            inoculation_od=inoculation_od,
            od_sample_vol=od_sample_vol,
            inoculation_volume_limit=inoculation_volume_limit,
        )

        insert_child_records(cursor, propagation_rows, run_timestamp)

        generate_sequence_files(
            run_id=run_id,
            propagation_rows=propagation_rows,
            output_dir=args.output_dir,
            layout_path=args.layout_path,
            source_labware=SOURCE_LABWARE_ID,
            child_labware=CHILD_LABWARE_ID,
        )

        conn.commit()
        conn.close()

        log("Committed database transaction.")
        log("=== 100% child-plate propagation script completed successfully ===")
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
