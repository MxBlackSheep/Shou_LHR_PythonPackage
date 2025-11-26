import pyodbc
import sys
import os
from datetime import datetime

LOG_DIR = r"C:\Python Log"
os.makedirs(LOG_DIR, exist_ok=True)

# ---------- DB ----------
def establish_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 11 for SQL Server};"
        "SERVER=LOCALHOST\\HAMILTON;"
        "DATABASE=EvoYeast;"
        "UID=Hamilton;PWD=mkdpw:V43;"
        "Trusted_Connection=no;"
        "TrustServerCertificate=yes;",
        autocommit=False
    )


def get_run_guid():
    with establish_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT TOP 1 RunGUID "
            "FROM HamiltonVectorDB.dbo.HxRun "
            "ORDER BY StartTime DESC"
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("No RunGUID found in HamiltonVectorDB.dbo.HxRun.")
        return str(row[0])


# ---------- Logging (created only after RunGUID is known) ----------
def init_logger(run_id: str):
    script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    log_path = os.path.join(LOG_DIR, f"{run_id}_{script_name}.log")

    def log(msg: str):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{now}] {msg}\n")

    return log, log_path


# ---------- Helpers ----------
def fq_tip_table(bucket: str) -> str:
    if bucket not in ("ColA", "ColB"):
        raise ValueError("bucket must be ColA or ColB")
    return f"[Labwares].[dbo].[TipUsage_{bucket}]"


def determine_bucket_order(cursor, log, left="ColA", right="ColB", threshold=384):
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
    def count_clean(bucket: str) -> int:
        tbl = fq_tip_table(bucket)
        cursor.execute(f"SELECT COUNT(*) FROM {tbl} WHERE status = ?", ("clean",))
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
            preferred, other = left, right
            reason = (
                f"both in-use (<{threshold}); preferring hard-coded left ({left})"
            )
        else:
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
        log(
            f"ERROR determining bucket order: {e}. "
            f"Defaulting to preferred={left}, other={right}"
        )
        return left, right


def count_available(cursor, bucket, from_status):
    tbl = fq_tip_table(bucket)
    cursor.execute(f"SELECT COUNT(*) FROM {tbl} WHERE status = ?", (from_status,))
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def update_top_n(cursor, bucket, from_status, to_status, take_n):
    if take_n <= 0:
        return 0
    tbl = fq_tip_table(bucket)
    sql = f"""
        WITH nextN AS (
            SELECT TOP ({take_n}) *
            FROM {tbl}
            WHERE status = ?
            ORDER BY order_id
        )
        UPDATE nextN SET status = ?;
    """
    cursor.execute(sql, (from_status, to_status))
    # rowcount can be -1 depending on driver; if so, assume take_n
    return cursor.rowcount if cursor.rowcount not in (-1, None) else take_n


# ---------- Main ----------
def main():
    # args
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <TipMask> <FromStatus> <ToStatus>")
        print(f"Example: {sys.argv[0]} 11111111 clean empty")
        sys.exit(2)

    tipmask = sys.argv[1].strip()
    from_status = sys.argv[2].strip().lower()
    to_status = sys.argv[3].strip().lower()

    # TipMask is only used for the COUNT of '1's
    if not all(c in "01" for c in tipmask):
        sys.exit(2)
    n = tipmask.count("1")
    if n <= 0:
        sys.exit(0)

    # 1) Get RunGUID first
    try:
        run_id = get_run_guid()
    except Exception as e:
        # Cannot log yet because logger depends on RunGUID
        print(f"ERROR getting RunGUID: {e}")
        sys.exit(1)

    # 2) Initialize logger based on RunGUID
    log, log_path = init_logger(run_id)
    log(f"RunGUID: {run_id}")
    log(
        f"TipMask = {tipmask} (count={n}) -> updating next {n} tips "
        f"from '{from_status}' to '{to_status}'"
    )

    conn = None
    cur = None
    exit_code = 0

    try:
        conn = establish_connection()
        cur = conn.cursor()

        # Use CLEAN-count–based priority logic to decide which column to update first
        preferred, other = determine_bucket_order(cur, log, left="ColA", right="ColB", threshold=384)

        pref_avail = count_available(cur, preferred, from_status)
        other_avail = count_available(cur, other, from_status)
        log(
            f"Available '{from_status}': "
            f"TipUsage_{preferred}={pref_avail}, TipUsage_{other}={other_avail}, "
            f"need={n}"
        )

        # Update preferred bucket first
        take_pref = min(n, pref_avail)
        updated_pref = update_top_n(cur, preferred, from_status, to_status, take_pref)
        remainder = n - updated_pref

        # Then update from the other bucket if needed
        updated_other = 0
        if remainder > 0:
            take_other = min(remainder, other_avail)
            updated_other = update_top_n(cur, other, from_status, to_status, take_other)

        total_updated = (updated_pref or 0) + (updated_other or 0)
        conn.commit()

        log(
            f"Updated {total_updated} rows from '{from_status}' to '{to_status}' "
            f"(TipUsage_{preferred}: {updated_pref}, "
            f"TipUsage_{other}: {updated_other}; requested {n})."
        )
        log("Commit successful.")

    except Exception as e:
        exit_code = 1
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        log(f"Fatal error: {e}")
        print(f"ERROR: {e}")
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
