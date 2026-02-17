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
        autocommit=True
    )


def get_runID():
    """
    Get the most recent RunGUID from HamiltonVectorDB.dbo.HxRun.
    """
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


def table_name_1000(bucket: str) -> str:
    if bucket not in ("ColA", "ColB"):
        raise ValueError("bucket must be ColA or ColB")
    return f"Labwares.dbo.TipUsage_{bucket}"


def table_name_300(bucket: str) -> str:
    if bucket not in ("ColA", "ColB"):
        raise ValueError("bucket must be ColA or ColB")
    return f"Labwares.dbo.TipUsage_300ul_{bucket}"


def reserved_to_unclear(cursor, full_table_name: str) -> int:
    """
    Convert reserved to unclear for a given table.
    Returns number of rows affected if available, otherwise returns 0.
    """
    sql = f"""
        UPDATE {full_table_name}
        SET status = N'unclear'
        WHERE status = N'reserved';
    """
    cursor.execute(sql)
    rc = cursor.rowcount
    if rc is None or rc == -1:
        rc = 0
    log(f"Updated reserved to unclear in {full_table_name}: rowcount={rc}")
    return rc


def determine_bucket_order(
    cursor,
    table_func,
    left="ColA",
    right="ColB",
    exclude_labwares=None,
    label="",
):
    """
    Corrected prioritisation logic:
      - Count CLEAN tips per column (optionally excluding labwares that should never be used)
      - If both columns have CLEAN tips, prioritise the one with fewer CLEAN tips
      - If only one has CLEAN tips, prioritise that one
      - If neither has CLEAN tips, default to left
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

    left_clean = count_clean(left)
    right_clean = count_clean(right)

    log(f"{label} CLEAN counts -> {left}: {left_clean}, {right}: {right_clean}, exclude={exclude_labwares}")

    if left_clean > 0 and right_clean > 0:
        if left_clean < right_clean:
            preferred, other = left, right
            reason = f"both have clean tips; fewer clean in {left}"
        elif right_clean < left_clean:
            preferred, other = right, left
            reason = f"both have clean tips; fewer clean in {right}"
        else:
            preferred, other = left, right
            reason = f"both have equal clean tips; defaulting to {left}"
    elif left_clean > 0:
        preferred, other = left, right
        reason = f"only {left} has clean tips"
    elif right_clean > 0:
        preferred, other = right, left
        reason = f"only {right} has clean tips"
    else:
        preferred, other = left, right
        reason = f"no clean tips in either; defaulting to {left}"

    log(f"{label} Determined bucket order: preferred={preferred}, other={other} ({reason})")
    return preferred, other


def fetch_clean(cursor, tables_in_order, table_func, exclude_labwares=None, label=""):
    """
    Fetch (Labware, Position) rows where status is clean, ordered by order_id.
    exclude_labwares can be a tuple like ("VER_HT_0009", "VER_HT_0010") or None.
    """
    results = []
    for bucket in tables_in_order:
        tbl = table_func(bucket)

        if exclude_labwares:
            placeholders = ",".join(["?"] * len(exclude_labwares))
            sql = f"""
                SELECT TU.labware_id AS Labware, TU.position_id AS Position
                FROM {tbl} AS TU
                WHERE TU.status = N'clean'
                  AND TU.labware_id NOT IN ({placeholders})
                ORDER BY TU.order_id;
            """
            cursor.execute(sql, exclude_labwares)
        else:
            sql = f"""
                SELECT TU.labware_id AS Labware, TU.position_id AS Position
                FROM {tbl} AS TU
                WHERE TU.status = N'clean'
                ORDER BY TU.order_id;
            """
            cursor.execute(sql)

        rows = cursor.fetchall()
        log(f"{label} Fetched CLEAN from {tbl}: {len(rows)} rows")
        results.extend(rows)

    return results


def fetch_available_non_dirty(cursor, tables_in_order, table_func, exclude_labwares=None, label=""):
    """
    "Dirty file" logic: choose tips that are NOT dirty, rinsed, washed.
    """
    results = []
    for bucket in tables_in_order:
        tbl = table_func(bucket)

        if exclude_labwares:
            placeholders = ",".join(["?"] * len(exclude_labwares))
            sql = f"""
                SELECT TU.labware_id AS Labware, TU.position_id AS Position
                FROM {tbl} AS TU
                WHERE TU.status NOT IN (N'dirty', N'rinsed', N'washed')
                  AND TU.labware_id NOT IN ({placeholders})
                ORDER BY TU.order_id;
            """
            cursor.execute(sql, exclude_labwares)
        else:
            sql = f"""
                SELECT TU.labware_id AS Labware, TU.position_id AS Position
                FROM {tbl} AS TU
                WHERE TU.status NOT IN (N'dirty', N'rinsed', N'washed')
                ORDER BY TU.order_id;
            """
            cursor.execute(sql)

        rows = cursor.fetchall()
        log(f"{label} Fetched NON-DIRTY from {tbl}: {len(rows)} rows")
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
        # Step 1: Safety recovery
        for tbl in (table_name_1000("ColA"), table_name_1000("ColB"), table_name_300("ColA"), table_name_300("ColB")):
            reserved_to_unclear(cursor, tbl)

        # Correct exclusions for CLEAN sequence construction
        # 1000ul excludes empty racks 0009 and 0010
        exclude_clean_1000 = ("VER_HT_0009", "VER_HT_0010")
        # 300ul excludes empty racks 0001 and 0004
        exclude_clean_300 = ("VER_ST_0009", "VER_ST_0010")

        # Step 2: Determine priority order for 1000ul tips using corrected rule
        pref_1000, other_1000 = determine_bucket_order(
            cursor,
            table_func=table_name_1000,
            left="ColA",
            right="ColB",
            exclude_labwares=exclude_clean_1000,
            label="1000ul",
        )
        tables_1000 = [pref_1000, other_1000]

        # Step 3: Determine priority order for 300ul tips using corrected rule
        pref_300, other_300 = determine_bucket_order(
            cursor,
            table_func=table_name_300,
            left="ColA",
            right="ColB",
            exclude_labwares=exclude_clean_300,
            label="300ul",
        )
        tables_300 = [pref_300, other_300]

        # Step 4: Fetch 1000ul sets
        clean_1000 = fetch_clean(
            cursor,
            tables_1000,
            table_func=table_name_1000,
            exclude_labwares=exclude_clean_1000,
            label="1000ul",
        )
        avail_1000 = fetch_available_non_dirty(
            cursor,
            tables_1000,
            table_func=table_name_1000,
            exclude_labwares=("VER_HT_0005", "VER_HT_0003"),
            label="1000ul",
        )

        # Step 5: Fetch 300ul sets
        clean_300 = fetch_clean(
            cursor,
            tables_300,
            table_func=table_name_300,
            exclude_labwares=exclude_clean_300,
            label="300ul",
        )
        avail_300 = fetch_available_non_dirty(
            cursor,
            tables_300,
            table_func=table_name_300,
            exclude_labwares=("VER_ST_0001", "VER_ST_0004"),
            label="300ul",
        )

        # Step 6: Paths / names
        run_id = get_runID()
        layout_path = (
            r"C:\PROGRAM FILES\HAMILTON\METHODS\LABPROTOCOLS\EXPERIMENTS\DECKS"
            r"\SPATIALEVOLUTION3OD384WELL.LAY"
        )

        out_clean_1000 = os.path.join(evotask_dir, f"{run_id}_Clean1000_Positions.txt")
        out_dirty_1000 = os.path.join(evotask_dir, f"{run_id}_Dirty1000_Positions.txt")
        out_clean_300 = os.path.join(evotask_dir, f"{run_id}_Clean300_Positions.txt")
        out_dirty_300 = os.path.join(evotask_dir, f"{run_id}_Dirty300_Positions.txt")

        # Step 7: Write files
        write_sequence(out_clean_1000, clean_1000, "Clean1000ulTips", layout_path)
        write_sequence(out_dirty_1000, avail_1000, "Dirty1000ulTips", layout_path)
        write_sequence(out_clean_300, clean_300, "Clean300ulTips", layout_path)
        write_sequence(out_dirty_300, avail_300, "Dirty300ulTips", layout_path)

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