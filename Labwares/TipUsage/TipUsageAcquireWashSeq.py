import pyodbc
import sys
import os
import csv
from datetime import datetime

# === Setup logging ===
log_dir = r"C:\Python Log"
evotask_dir = r"C:\EvoTaskFiles"
os.makedirs(log_dir, exist_ok=True)
os.makedirs(evotask_dir, exist_ok=True)

script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(log_dir, f"{script_name}_{timestamp}.log")


def log(msg):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {msg}\n")


def establish_connection():
    # Use EvoYeast DB for the session; we will query Labwares.dbo.* fully qualified
    return pyodbc.connect(
        "DRIVER={ODBC Driver 11 for SQL Server};"
        "SERVER=LOCALHOST\\HAMILTON;"
        "DATABASE=EvoYeast;"
        "UID=Hamilton;PWD=mkdpw:V43;Trust_Connection=no;TrustServerCertificate=yes;",
        autocommit=True
    )


def get_runID():
    try:
        conn = establish_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT TOP 1 RunGUID "
            "FROM HamiltonVectorDB.dbo.HxRun "
            "ORDER BY StartTime DESC"
        )
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


def determine_bucket_order(cursor, left="ColA", right="ColB", threshold=384):
    """
    Determine which column (bucket) to treat as 'preferred' based on the number
    of CLEAN tips in each TipUsage table.

    Logic:
    - Count rows WHERE status = 'clean' in TipUsage_left and TipUsage_right.
    - Prefer the table where clean_count < threshold (96 * 4) as an indicator
      that this column is currently being used.
    - If both have clean_count < threshold, prefer the hard-coded 'left' column.
    - If both have clean_count >= threshold, default to the hard-coded 'left'
      column but log that we’re above threshold in both.

    Returns:
        (preferred_bucket, other_bucket)
        e.g. ('ColA', 'ColB') or ('ColB', 'ColA')
    """
    def count_clean(bucket_name: str) -> int:
        sql = f"""
            SELECT COUNT(*)
            FROM Labwares.dbo.TipUsage_{bucket_name}
            WHERE status = N'clean';
        """
        cursor.execute(sql)
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    try:
        left_clean = count_clean(left)
        right_clean = count_clean(right)

        log(
            f"CLEAN counts -> {left}: {left_clean}, {right}: {right_clean}, "
            f"threshold={threshold}"
        )

        left_in_use = left_clean < threshold
        right_in_use = right_clean < threshold

        if left_in_use and not right_in_use:
            preferred, other = left, right
            reason = f"{left} in-use (<{threshold}), {right} not in-use"
        elif right_in_use and not left_in_use:
            preferred, other = right, left
            reason = f"{right} in-use (<{threshold}), {left} not in-use"
        elif left_in_use and right_in_use:
            # Both "in use" – prefer the hard-coded left
            preferred, other = left, right
            reason = (
                f"both in-use (<{threshold}); preferring hard-coded left ({left})"
            )
        else:
            # Both >= threshold – no obvious 'current' column; default to left
            preferred, other = left, right
            reason = (
                f"both >= {threshold}; defaulting to hard-coded left ({left})"
            )

        log(
            f"Determined bucket order: preferred={preferred}, other={other} "
            f"({reason})"
        )
        return preferred, other

    except Exception as e:
        # If anything goes wrong, fall back to left as preferred
        log(
            f"ERROR determining bucket order: {e}. "
            f"Defaulting to preferred={left}, other={right}"
        )
        return left, right


def fetch_wash(cursor, tables_in_order):
    """
    tables_in_order: e.g. ['ColB','ColA'] meaning query TipUsage_ColB first, then _ColA
    Returns list of (Labware, Position)
    Fetch all Positions TO BE WASHED (status = 'dirty').
    """
    results = []
    for bucket in tables_in_order:
        sql = f"""
            SELECT TU.labware_id AS Labware, TU.position_id AS Position
            FROM Labwares.dbo.TipUsage_{bucket} AS TU
            WHERE TU.status = N'dirty'
            ORDER BY TU.order_id;
        """
        cursor.execute(sql)
        rows = cursor.fetchall()
        log(f"Fetched Wash from TipUsage_{bucket}: {len(rows)} rows")
        results.extend(rows)
    return results


def write_sequence(path, rows, sequence_name, layout_path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Id", "Layout", "Sequence", "Labware", "Position"])
        for idx, r in enumerate(rows, start=1):
            # r can be pyodbc.Row or tuple
            lab = r[0]
            pos = int(r[1])
            w.writerow([idx, layout_path, sequence_name, lab, pos])
    log(f"Wrote {len(rows)} lines -> {path}")


def main():
    conn = establish_connection()
    cursor = conn.cursor()
    log("Connection Established")

    exit_code = 0

    try:
        # Determine priority order between columns based on CLEAN counts
        pref, other = determine_bucket_order(cursor, left="ColA", right="ColB", threshold=384)
        tables_in_order = [pref, other]  # e.g., ['ColA','ColB']

        wash_rows = fetch_wash(cursor, tables_in_order)
        run_id = get_runID()
        layout_path = (
            r"C:\PROGRAM FILES\HAMILTON\METHODS\LABPROTOCOLS\EXPERIMENTS\DECKS"
            r"\SPATIALEVOLUTION3OD384WELL.LAY"
        )
        out_wash = os.path.join(evotask_dir, f"{run_id}_Wash1000_Positions.txt")
        write_sequence(out_wash, wash_rows, "Dirty1000ulTips", layout_path)

    except Exception as e:
        log(f"Fatal error: {e}")
        exit_code = 1
    finally:
        try:
            conn.close()
            log("Connection Closed")
        except Exception as e:
            log(f"ERROR closing connection: {e}")
            if exit_code == 0:
                exit_code = 1
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
