"""SQL connections and lookups shared by VENUS workflows.

Callers own commit boundaries. Merely leaving a connection context never commits.
"""

from contextlib import contextmanager
import pyodbc

from .config import Settings
from .runtime import discard_log


def connect(settings: Settings):
    return pyodbc.connect(
        f"DRIVER={{{settings.odbc_driver}}};"
        f"SERVER={settings.server};DATABASE={settings.database};"
        f"UID={settings.username};PWD={settings.password};Trust_Connection=no;"
    )


@contextmanager
def connection(settings: Settings):
    conn = connect(settings)
    try:
        yield conn
    except BaseException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def fetch_one_value(cursor, sql, params=()):
    if params:
        cursor.execute(sql, tuple(params))
    else:
        cursor.execute(sql)
    row = cursor.fetchone()
    return None if row is None else row[0]


def latest_run_id(cursor):
    value = fetch_one_value(
        cursor,
        "SELECT TOP 1 RunGUID FROM HamiltonVectorDB.dbo.HxRun ORDER BY StartTime DESC",
    )
    if not value:
        raise RuntimeError("No RunGUID found in HamiltonVectorDB.dbo.HxRun")
    return value


def plate_id_from_barcode(cursor, barcode):
    value = fetch_one_value(cursor, "SELECT PlateID FROM Plates WHERE BarCode = ?", (barcode,))
    if value is None:
        raise RuntimeError(f"No PlateID found for barcode {barcode}.")
    return value


def latest_ancestor_plate_id(cursor):
    value = fetch_one_value(
        cursor, "SELECT TOP 1 PlateID FROM AncestPlatesInExperiments ORDER BY ExperimentID DESC"
    )
    if value is None:
        raise RuntimeError("No ancestor PlateID found.")
    return value


def fetch_all_result_sets(cursor, *, skip_row=None, log=discard_log):
    """Collect SELECTs across a procedure, including after non-row result sets.

    Optional row filtering belongs to the caller's stored-procedure contract.
    """
    collected = []
    index = 0
    while True:
        index += 1
        try:
            rows = cursor.fetchall()
            log(f"Result set {index}: fetched {len(rows)} rows.")
            collected.extend(row for row in rows if skip_row is None or not skip_row(row))
        except pyodbc.ProgrammingError as error:
            log(f"Result set {index}: no fetchable rows. {error}")
        if not cursor.nextset():
            break
    log(f"Total rows collected across all result sets: {len(collected)}")
    return collected
