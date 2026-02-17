import pyodbc
import sys
import os
from datetime import datetime
from typing import List, Tuple


LOG_DIR = r"C:\Python Log"
os.makedirs(LOG_DIR, exist_ok=True)

SCRIPT_NAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_DIR, f"{SCRIPT_NAME}_{TIMESTAMP}.log")


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {msg}\n")


def establish_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 11 for SQL Server};"
        "SERVER=LOCALHOST\\HAMILTON;"
        "DATABASE=EvoYeast;"
        "UID=Hamilton;PWD=mkdpw:V43;"
        "Trusted_Connection=no;"
        "TrustServerCertificate=yes;",
        autocommit=True,
    )


def parse_tip_type(arg: str) -> str:
    s = arg.strip().lower()
    s = s.replace("µl", "").replace("μl", "").replace("ul", "").strip()
    if s == "1000":
        return "1000"
    if s == "300":
        return "300"
    raise ValueError(f"Unrecognised tip type: {arg}. Use 1000 or 300.")


def table_name_1000(bucket: str) -> str:
    if bucket not in ("ColA", "ColB"):
        raise ValueError("bucket must be ColA or ColB")
    return f"Labwares.dbo.TipUsage_{bucket}"


def table_name_300(bucket: str) -> str:
    if bucket not in ("ColA", "ColB"):
        raise ValueError("bucket must be ColA or ColB")
    return f"Labwares.dbo.TipUsage_300ul_{bucket}"


def determine_prioritised_bucket(cursor, table_func, exclude_labwares: Tuple[str, ...], label: str = "") -> str:
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

    log(f"{label} clean counts (excluding {exclude_labwares}): ColA={clean_a}, ColB={clean_b}")

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

    log(f"{label} prioritised bucket: {preferred} ({reason})")
    return preferred

def update_status_by_labwares(cursor, full_table_name: str, labwares: List[str], status: str) -> int:
    if not labwares:
        return 0

    placeholders = ",".join(["?"] * len(labwares))
    sql = f"""
        UPDATE {full_table_name}
        SET status = ?
        WHERE labware_id IN ({placeholders});
    """
    params = [status] + labwares
    cursor.execute(sql, params)

    rc = cursor.rowcount
    if rc is None or rc == -1:
        rc = 0

    log(f"Set status={status} in {full_table_name} for {len(labwares)} labwares, rowcount={rc}")
    return rc


def main() -> None:
    exit_code = 0
    conn = None

    try:
        if len(sys.argv) < 2:
            raise RuntimeError("Missing tip type argument. Use 1000 or 300.")

        tip_type = parse_tip_type(sys.argv[1])
        log(f"Starting reset programme, tip_type={tip_type}")

        if tip_type == "1000":
            table_func = table_name_1000
            label = "1000ul"
            exclude_labwares = ("VER_HT_0009", "VER_HT_0010")
        else:
            table_func = table_name_300
            label = "300ul"
            exclude_labwares = ("VER_ST_0009", "VER_ST_0010")

        conn = establish_connection()
        cursor = conn.cursor()
        log("Connection established")

        prioritised = determine_prioritised_bucket(
            cursor,
            table_func=table_func,
            exclude_labwares=exclude_labwares,
            label=label,
        )

        other = "ColB" if prioritised == "ColA" else "ColA"
        tbl_other = table_func(other)

        log(f"{label} prioritised={prioritised}, resetting other column={other} ({tbl_other})")

        if tip_type == "1000":
            reset_map = {
                "ColA": {
                    "clean": ["VER_HT_0005", "VER_HT_0001", "VER_HT_0002", "VER_HT_0006"],
                    "empty": ["VER_HT_0009"],
                },
                "ColB": {
                    "clean": ["VER_HT_0003", "VER_HT_0004", "VER_HT_0007", "VER_HT_0008"],
                    "empty": ["VER_HT_0010"],
                },
            }
        else:
            reset_map = {
                "ColA": {
                    "clean": ["VER_ST_0001", "VER_ST_0006", "VER_ST_0003", "VER_ST_0002"],
                    "empty": ["VER_ST_0009"],
                },
                "ColB": {
                    "clean": ["VER_ST_0004", "VER_ST_0008", "VER_ST_0007", "VER_ST_0005"],
                    "empty": ["VER_ST_0010"],
                },
            }

        labwares_clean = reset_map[other]["clean"]
        labwares_empty = reset_map[other]["empty"]

        update_status_by_labwares(cursor, tbl_other, labwares_clean, "clean")
        update_status_by_labwares(cursor, tbl_other, labwares_empty, "empty")

        log(f"{label} reset completed for {other}")

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