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


def determine_bucket_order(cursor, table_prefix, left="ColA", right="ColB", threshold=384, label="1000ul"):
    """
    Determine preferred bucket based on CLEAN counts in:
      Labwares.dbo.{table_prefix}{ColA/ColB}

    table_prefix examples:
      - "TipUsage_"            -> TipUsage_ColA / TipUsage_ColB
      - "TipUsage_300ul_"      -> TipUsage_300ul_ColA / TipUsage_300ul_ColB
    """

    def count_clean(bucket_name):
        sql = f"""
            SELECT COUNT(*)
            FROM Labwares.dbo.{table_prefix}{bucket_name}
            WHERE status = N'clean';
        """
        cursor.execute(sql)
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    try:
        left_clean = count_clean(left)
        right_clean = count_clean(right)

        log(
            f"{label} CLEAN counts -> {left}: {left_clean}, {right}: {right_clean}, "
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
            preferred, other = left, right
            reason = f"both in-use (<{threshold}); preferring hard-coded left ({left})"
        else:
            preferred, other = left, right
            reason = f"both >= {threshold}; defaulting to hard-coded left ({left})"

        log(
            f"{label} Determined bucket order: preferred={preferred}, other={other} "
            f"({reason})"
        )
        return preferred, other

    except Exception as e:
        log(
            f"{label} ERROR determining bucket order: {e}. "
            f"Defaulting to preferred={left}, other={right}"
        )
        return left, right


def fetch_rinse(cursor, table_prefix, tables_in_order, label="1000ul"):
    """
    Fetch all Positions TO BE RINSED (status = 'dirty') from:
      Labwares.dbo.{table_prefix}{ColA/ColB}

    Returns list of (Labware, Position).
    """
    results = []
    for bucket in tables_in_order:
        sql = f"""
            SELECT TU.labware_id AS Labware, TU.position_id AS Position
            FROM Labwares.dbo.{table_prefix}{bucket} AS TU
            WHERE TU.status = N'dirty'
            ORDER BY TU.order_id;
        """
        cursor.execute(sql)
        rows = cursor.fetchall()
        log(f"{label} Fetched Rinse from {table_prefix}{bucket}: {len(rows)} rows")
        results.extend(rows)
    return results


def write_sequence(path, rows, sequence_name, layout_path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Id", "Layout", "Sequence", "Labware", "Position"])
        for idx, r in enumerate(rows, start=1):
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
        run_id = get_runID()

        layout_path = (
            r"C:\PROGRAM FILES\HAMILTON\METHODS\LABPROTOCOLS\EXPERIMENTS\DECKS"
            r"\SPATIALEVOLUTION3OD384WELL.LAY"
        )

        # ---------------- 1000ul (existing) ----------------
        pref_1000, other_1000 = determine_bucket_order(
            cursor,
            table_prefix="TipUsage_",
            left="ColA",
            right="ColB",
            threshold=384,
            label="1000ul",
        )
        tables_1000 = [pref_1000, other_1000]
        rinse_1000 = fetch_rinse(cursor, "TipUsage_", tables_1000, label="1000ul")

        out_rinse_1000 = os.path.join(evotask_dir, f"{run_id}_Rinse1000_Positions.txt")
        write_sequence(out_rinse_1000, rinse_1000, "Dirty1000ulTips", layout_path)

        # ---------------- 300ul (new) ----------------
        pref_300, other_300 = determine_bucket_order(
            cursor,
            table_prefix="TipUsage_300ul_",
            left="ColA",
            right="ColB",
            threshold=384,
            label="300ul",
        )
        tables_300 = [pref_300, other_300]
        rinse_300 = fetch_rinse(cursor, "TipUsage_300ul_", tables_300, label="300ul")

        out_rinse_300 = os.path.join(evotask_dir, f"{run_id}_Rinse300_Positions.txt")
        write_sequence(out_rinse_300, rinse_300, "Dirty300ulTips", layout_path)

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
