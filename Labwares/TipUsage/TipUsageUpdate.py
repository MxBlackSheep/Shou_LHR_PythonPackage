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


def build_exclusion_for_status(from_status: str):
    """
    Return labware_ids to exclude, using the same rules as the selection code.
      - From CLEAN tips - Dirty Tips: skip VER_HT_0009 and VER_HT_0010
      - From Empty position - Dirty Tips: skip VER_HT_0005 and VER_HT_0003
    """
    if from_status == "clean":
        return ("VER_HT_0009", "VER_HT_0010")
    if from_status == "empty":
        return ("VER_HT_0005", "VER_HT_0003")
    return None


def determine_bucket_order(cursor, log, left="ColA", right="ColB", threshold=384):
    """
    Determine which column (bucket) to treat as 'preferred' based on the number
    of CLEAN tips in each TipUsage table.
    """
    def count_clean(bucket: str) -> int:
        tbl = fq_tip_table(bucket)
        sql = (
            f"SELECT COUNT(*) FROM {tbl} "
            f"WHERE status = N'clean' AND labware_id NOT IN (?, ?)"
        )
        cursor.execute(sql, ("VER_HT_0009", "VER_HT_0010"))
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    try:
        left_clean = count_clean(left)
        right_clean = count_clean(right)

        log(
            f"CLEAN counts (excluding VER_HT_0009/0010) -> "
            f"{left}: {left_clean}, {right}: {right_clean}, "
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


def count_available(cursor, bucket, from_status, exclude_labwares=None):
    """
    Count how many tips in a bucket are in from_status, optionally skipping
    specific labware_ids (e.g. VER_HT_0009/00010 for clean).
    """
    tbl = fq_tip_table(bucket)
    params = [from_status]
    extra_where = ""
    if exclude_labwares:
        placeholders = ",".join("?" * len(exclude_labwares))
        extra_where = f" AND labware_id NOT IN ({placeholders})"
        params.extend(exclude_labwares)

    sql = f"SELECT COUNT(*) FROM {tbl} WHERE status = ?{extra_where}"
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def update_top_n(cursor, bucket, from_status, to_status, take_n, exclude_labwares=None):
    """
    Update the top-N tips (by order_id) from from_status -> to_status,
    optionally skipping specific labwares.
    """
    if take_n <= 0:
        return 0
    tbl = fq_tip_table(bucket)

    params = [from_status]
    extra_where = ""
    if exclude_labwares:
        placeholders = ",".join("?" * len(exclude_labwares))
        extra_where = f" AND labware_id NOT IN ({placeholders})"
        params.extend(exclude_labwares)

    sql = f"""
        WITH nextN AS (
            SELECT TOP ({take_n}) *
            FROM {tbl}
            WHERE status = ?{extra_where}
            ORDER BY order_id
        )
        UPDATE nextN SET status = ?;
    """
    params.append(to_status)
    cursor.execute(sql, params)
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

        # Decide labware exclusion based on from_status
        exclude_labwares = build_exclusion_for_status(from_status)
        if exclude_labwares:
            log(f"Excluding labwares {exclude_labwares} for from_status='{from_status}'")
        else:
            log(f"No labware exclusion for from_status='{from_status}'")

        # Use CLEAN-count–based priority logic to decide which column to update first
        preferred, other = determine_bucket_order(
            cur, log, left="ColA", right="ColB", threshold=384
        )

        pref_avail = count_available(cur, preferred, from_status, exclude_labwares)
        other_avail = count_available(cur, other, from_status, exclude_labwares)
        log(
            f"Available '{from_status}' (respecting exclusions): "
            f"TipUsage_{preferred}={pref_avail}, TipUsage_{other}={other_avail}, "
            f"need={n}"
        )

        # Update preferred bucket first
        take_pref = min(n, pref_avail)
        updated_pref = update_top_n(
            cur, preferred, from_status, to_status, take_pref, exclude_labwares
        )
        remainder = n - updated_pref

        # Then update from the other bucket if needed
        updated_other = 0
        if remainder > 0:
            take_other = min(remainder, other_avail)
            updated_other = update_top_n(
                cur, other, from_status, to_status, take_other, exclude_labwares
            )

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
