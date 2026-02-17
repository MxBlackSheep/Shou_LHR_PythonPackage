import pyodbc
import sys
import os
import re
from datetime import datetime
from collections import Counter

LOG_DIR = r"C:\Python Log"
os.makedirs(LOG_DIR, exist_ok=True)

ALLOWED_TIP_TYPES = {"300", "1000"}
BUCKETS = ("ColA", "ColB")
KEY_COL = "position_id"


def establish_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 11 for SQL Server};"
        r"SERVER=LOCALHOST\HAMILTON;"
        "DATABASE=EvoYeast;"
        "UID=Hamilton;PWD=mkdpw:V43;"
        "Trusted_Connection=no;"
        "TrustServerCertificate=yes;",
        autocommit=False,
    )


def get_latest_run_guid():
    try:
        with establish_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT TOP 1 RunGUID "
                "FROM HamiltonVectorDB.dbo.HxRun "
                "ORDER BY StartTime DESC"
            )
            row = cur.fetchone()
            return str(row[0]) if row else None
    except Exception:
        return None


def make_logger(run_id: str):
    script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    log_path = os.path.join(LOG_DIR, f"{run_id}_{script_name}.log")

    def log(msg: str):
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"[{now}] {msg}\n")
        except Exception:
            pass

    return log


def tip_table(bucket: str, tip_type: str):
    if bucket not in BUCKETS:
        return None
    if tip_type not in ALLOWED_TIP_TYPES:
        return None
    if tip_type == "300":
        return f"[Labwares].[dbo].[TipUsage_300ul_{bucket}]"
    return f"[Labwares].[dbo].[TipUsage_{bucket}]"


def parse_positions(raw: str):
    """
    Input example:
      VER_HT_0001::1--VER_HT_0001::2--VER_HT_0001::3--

    Returns list of (labware_id, position_id) tuples.
    De-duplicates while preserving order.
    """
    if not raw:
        return []

    s = raw.strip()
    if not s:
        return []

    for sep in [",", ";", "|", "\n", "\r", "\t", " "]:
        s = s.replace(sep, "-")

    parts = [p.strip() for p in re.split(r"-+", s) if p.strip()]

    out = []
    seen = set()
    for p in parts:
        if "::" not in p:
            continue
        labware_id, pos = p.split("::", 1)
        labware_id = labware_id.strip()
        pos = pos.strip()
        if not labware_id or not pos:
            continue
        try:
            position_id = int(pos)
        except Exception:
            continue

        key = (labware_id, position_id)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)

    return out


def build_pos_cte(positions):
    values_sql = ",".join(["(?, ?)"] * len(positions))
    cte = (
        f"WITH pos(labware_id, {KEY_COL}) AS ("
        f" SELECT * FROM (VALUES {values_sql}) AS v(labware_id, {KEY_COL})"
        ")"
    )
    params = []
    for lw, pid in positions:
        params.extend([lw, pid])
    return cte, params


def count_in_bucket(cur, table, positions):
    if not positions:
        return 0

    cte, params = build_pos_cte(positions)
    sql = f"""
        {cte}
        SELECT COUNT(*)
        FROM {table} t
        INNER JOIN pos p
            ON p.labware_id = t.labware_id
           AND p.{KEY_COL} = t.{KEY_COL};
    """
    cur.execute(sql, params)
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def get_status_counter_in_bucket(cur, table, positions):
    """
    Returns Counter of current statuses for the requested positions in this table.
    Only counts rows that exist in this table.
    """
    if not positions:
        return Counter()

    cte, params = build_pos_cte(positions)
    sql = f"""
        {cte}
        SELECT t.status
        FROM {table} t
        INNER JOIN pos p
            ON p.labware_id = t.labware_id
           AND p.{KEY_COL} = t.{KEY_COL};
    """
    cur.execute(sql, params)
    statuses = [str(r[0]).lower() for r in (cur.fetchall() or [])]
    return Counter(statuses)


def update_in_bucket(cur, table, positions, to_status):
    """
    Update rows in one bucket table by labware_id + position_id only.
    Returns number of rows updated.
    """
    if not positions:
        return 0

    cte, params = build_pos_cte(positions)
    sql = f"""
        {cte}
        UPDATE t
        SET t.status = ?
        FROM {table} t
        INNER JOIN pos p
            ON p.labware_id = t.labware_id
           AND p.{KEY_COL} = t.{KEY_COL};
    """
    cur.execute(sql, params + [to_status])

    if cur.rowcount not in (-1, None):
        return int(cur.rowcount)

    # Fallback: rowcount may be -1 for some drivers/settings
    # We can re-count existence; update should not change existence, so return 0
    # and let the strict updated_total check fail (safer).
    return 0


def log_status_dump(cur, tables_by_bucket, positions, log):
    if not positions:
        return

    cte, params = build_pos_cte(positions)
    rows = []

    for bucket, table in tables_by_bucket.items():
        sql = f"""
            {cte}
            SELECT t.labware_id, t.{KEY_COL}, t.status
            FROM {table} t
            INNER JOIN pos p
                ON p.labware_id = t.labware_id
               AND p.{KEY_COL} = t.{KEY_COL};
        """
        cur.execute(sql, params)
        for r in cur.fetchall() or []:
            rows.append((str(r[0]), int(r[1]), str(r[2]), bucket))

    rows.sort(key=lambda x: (x[0], x[1], x[3]))

    log("Status dump begin")
    for lw, pid, st, bucket in rows:
        log(f"Status {bucket} {lw} {pid} status={st}")
    log("Status dump end")


def print_usage(script):
    print(f"Usage: {script} <Positions> <ToStatus> <TipType>")
    print(f"Example: {script} \"VER_HT_0001::1-VER_HT_0001::2\" empty 1000")


def main():
    if len(sys.argv) != 4:
        print_usage(sys.argv[0])
        sys.exit(1)

    raw_positions = (sys.argv[1] or "").strip()
    to_status = (sys.argv[2] or "").strip().lower()
    tip_type = (sys.argv[3] or "").strip()

    if tip_type not in ALLOWED_TIP_TYPES:
        print(f"ERROR: TipType must be one of {sorted(ALLOWED_TIP_TYPES)}")
        sys.exit(1)

    positions = parse_positions(raw_positions)
    if not positions:
        print("ERROR: Positions string did not contain any valid labware_id::position_id entries")
        sys.exit(1)

    run_id = get_latest_run_guid()
    if not run_id:
        print("ERROR getting RunGUID")
        sys.exit(1)

    log = make_logger(run_id)
    log(f"RunGUID: {run_id}")
    log(f"TipType: {tip_type}")
    log(f"ToStatus={to_status}")
    log(f"Positions received: {len(positions)}")
    log(f"RawPositions={raw_positions}")

    tables = {
        "ColA": tip_table("ColA", tip_type),
        "ColB": tip_table("ColB", tip_type),
    }
    if not tables["ColA"] or not tables["ColB"]:
        log("ERROR: Could not resolve table names")
        sys.exit(1)

    conn = None
    cur = None

    try:
        conn = establish_connection()
        cur = conn.cursor()

        # 1) Strict existence check
        exist_total = sum(count_in_bucket(cur, tables[b], positions) for b in BUCKETS)
        log(f"Existence total={exist_total} requested={len(positions)}")
        if exist_total != len(positions):
            log_status_dump(cur, tables, positions, log)
            log("ERROR: Some requested positions not found. Rolling back.")
            conn.rollback()
            sys.exit(1)

        # 2) Log current statuses (no OUTPUT, compatible with triggers)
        before_counter = Counter()
        for b in BUCKETS:
            before_counter.update(get_status_counter_in_bucket(cur, tables[b], positions))
        log(f"Before-status breakdown: {dict(before_counter)}")

        # 3) Update by labware_id + position_id only
        updated_total = sum(update_in_bucket(cur, tables[b], positions, to_status) for b in BUCKETS)
        log(f"Updated total={updated_total} requested={len(positions)}")

        if updated_total != len(positions):
            log_status_dump(cur, tables, positions, log)
            log("ERROR: Updated count mismatch. Rolling back.")
            conn.rollback()
            sys.exit(1)

        conn.commit()
        log("Commit successful.")
        sys.exit(0)

    except Exception as e:
        try:
            log(f"Fatal error: {type(e).__name__}: {e}")
        except Exception:
            pass
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        sys.exit(1)

    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()