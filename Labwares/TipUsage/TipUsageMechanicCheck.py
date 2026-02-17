import pyodbc
import sys
import os
import csv
from datetime import datetime


LOG_DIR = r"C:\Python Log"
EVOTASK_DIR = r"C:\EvoTaskFiles"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(EVOTASK_DIR, exist_ok=True)

SCRIPT_NAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_DIR, f"{SCRIPT_NAME}_{TIMESTAMP}.log")

LAYOUT_PATH_DEFAULT = (
    r"C:\PROGRAM FILES\HAMILTON\METHODS\LABPROTOCOLS\EXPERIMENTS\DECKS"
    r"\SPATIALEVOLUTION3OD384WELL.LAY"
)


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {msg}\n")


def establish_connection():
    """
    Use EvoYeast DB for the session; we will query Labwares.dbo.* fully qualified.
    """
    return pyodbc.connect(
        "DRIVER={ODBC Driver 11 for SQL Server};"
        "SERVER=LOCALHOST\\HAMILTON;"
        "DATABASE=EvoYeast;"
        "UID=Hamilton;PWD=mkdpw:V43;"
        "Trusted_Connection=no;"
        "TrustServerCertificate=yes;",
        autocommit=True,
    )


def get_run_id() -> str:
    """
    Get the most recent RunGUID from HamiltonVectorDB.dbo.HxRun.
    """
    conn = None
    try:
        conn = establish_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT TOP 1 RunGUID "
            "FROM HamiltonVectorDB.dbo.HxRun "
            "ORDER BY StartTime DESC"
        )
        row = cursor.fetchone()
        if not row:
            raise RuntimeError("No RunGUID found in HamiltonVectorDB.dbo.HxRun")
        run_id = str(row[0])
        log(f"Retrieved RunGUID: {run_id}")
        return run_id
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def table_name_1000(bucket: str) -> str:
    if bucket not in ("ColA", "ColB"):
        raise ValueError("bucket must be ColA or ColB")
    return f"Labwares.dbo.TipUsage_{bucket}"


def table_name_300(bucket: str) -> str:
    if bucket not in ("ColA", "ColB"):
        raise ValueError("bucket must be ColA or ColB")
    return f"Labwares.dbo.TipUsage_300ul_{bucket}"


def parse_tip_type(arg: str) -> str:
    """
    Acceptable inputs: 1000, 1000ul, 1000uL, 300, 300ul, 300uL
    Returns normalised tip type key: "1000" or "300"
    """
    s = arg.strip().lower()
    s = s.replace("ul", "").replace("µl", "").replace("μl", "").strip()
    if s in ("1000", "1000u", "1000l"):
        return "1000"
    if s in ("300", "300u", "300l"):
        return "300"
    raise ValueError(f"Unrecognised tip type: {arg}")


def determine_prioritised_bucket(cursor, table_func, exclude_labwares, label: str = "") -> str:
    """
    Corrected prioritisation logic:
      If both columns have clean tips, prioritise the column with fewer clean tips.
      If only one has clean tips, prioritise that one.
      If neither has clean tips, default to ColA.

    exclude_labwares should be the labwares that are structurally "empty racks" for this tip type.
    """
    def count_clean(bucket_name: str) -> int:
        tbl = table_func(bucket_name)

        if exclude_labwares:
            placeholders = ",".join(["?"] * len(exclude_labwares))
            sql = f"""
                SELECT COUNT(*)
                FROM {tbl}
                WHERE status = N'clean'
                  AND labware_id NOT IN ({placeholders});
            """
            cursor.execute(sql, exclude_labwares)
        else:
            sql = f"SELECT COUNT(*) FROM {tbl} WHERE status = N'clean';"
            cursor.execute(sql)

        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    clean_a = count_clean("ColA")
    clean_b = count_clean("ColB")

    log(f"{label} CLEAN counts: ColA={clean_a}, ColB={clean_b}, exclude={exclude_labwares}")

    if clean_a > 0 and clean_b > 0:
        if clean_a < clean_b:
            preferred = "ColA"
            reason = "both have clean tips; fewer clean in ColA"
        elif clean_b < clean_a:
            preferred = "ColB"
            reason = "both have clean tips; fewer clean in ColB"
        else:
            preferred = "ColA"
            reason = "both have equal clean tips; tie break to ColA"
    elif clean_a > 0:
        preferred = "ColA"
        reason = "only ColA has clean tips"
    elif clean_b > 0:
        preferred = "ColB"
        reason = "only ColB has clean tips"
    else:
        preferred = "ColA"
        reason = "no clean tips in either; default ColA"

    log(f"{label} Prioritised bucket: {preferred} ({reason})")
    return preferred


def write_probe_sequence(path: str, labware_id: str, layout_path: str, sequence_name: str) -> None:
    """
    Write an 8-position probe sequence (positions 1 to 8) for a given labware.
    """
    rows = [(labware_id, pos) for pos in range(1, 9)]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Id", "Layout", "Sequence", "Labware", "Position"])
        for idx, (lab, pos) in enumerate(rows, start=1):
            w.writerow([idx, layout_path, sequence_name, lab, int(pos)])
    log(f"Wrote probe sequence: labware={labware_id}, positions=1..8, file={path}")


def main() -> None:
    """
    Usage:
      TipProbeSequence.exe 1000
      TipProbeSequence.exe 300
    Optional:
      TipProbeSequence.exe 1000 "C:\\path\\to\\layout.LAY"
    """
    exit_code = 0
    conn = None

    try:
        if len(sys.argv) < 2:
            raise RuntimeError("Missing tip type argument. Use 1000 or 300.")

        tip_type = parse_tip_type(sys.argv[1])
        layout_path = sys.argv[2] if len(sys.argv) >= 3 else LAYOUT_PATH_DEFAULT

        log(f"Starting probe generator, tip_type={tip_type}, layout={layout_path}")

        if tip_type == "1000":
            table_func = table_name_1000
            label = "1000ul"
            sequence_name = "Clean1000ulTips"
            exclude_labwares = ("VER_HT_0009", "VER_HT_0010")  # empty racks
            probe_map = {
                "ColA": "VER_HT_0003",  # prioritised ColA, probe ColB last rack
                "ColB": "VER_HT_0005",  # prioritised ColB, probe ColA last rack
            }
        else:
            table_func = table_name_300
            label = "300ul"
            sequence_name = "Clean300ulTips"
            exclude_labwares = ("VER_ST_0009", "VER_ST_0010")  # empty racks
            probe_map = {
                "ColA": "VER_ST_0004",  # prioritised ColA, probe ColB last rack
                "ColB": "VER_ST_0001",  # prioritised ColB, probe ColA last rack
            }

        conn = establish_connection()
        cursor = conn.cursor()
        log("Connection established")

        prioritised = determine_prioritised_bucket(
            cursor,
            table_func=table_func,
            exclude_labwares=exclude_labwares,
            label=label,
        )
        probe_labware = probe_map[prioritised]

        run_id = get_run_id()
        out_path = os.path.join(EVOTASK_DIR, f"{run_id}_Probe{tip_type}_Positions.txt")

        write_probe_sequence(out_path, probe_labware, layout_path, sequence_name)

    except Exception as e:
        log(f"Fatal error: {e}")
        exit_code = 1
    finally:
        if conn is not None:
            try:
                conn.close()
                log("Connection closed")
            except Exception as e:
                log(f"Error closing connection: {e}")
                if exit_code == 0:
                    exit_code = 1

        sys.exit(exit_code)


if __name__ == "__main__":
    main()