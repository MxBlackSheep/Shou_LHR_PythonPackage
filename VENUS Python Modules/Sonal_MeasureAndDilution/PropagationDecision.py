import pyodbc
import sys
import os
from datetime import datetime


# ============================================================
# Logging Setup
# ============================================================

log_dir = r"C:\Python Log"
os.makedirs(log_dir, exist_ok=True)

script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(log_dir, f"{script_name}_{timestamp}.log")


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {msg}\n")


# ============================================================
# Constants
# ============================================================

EVOTASK_DIR = r"C:\EvoTaskFiles"

# Output file contains a single value:
#   1 = propagate
#   0 = do not propagate
OUTPUT_SUFFIX = "PropagationDecision"

PROPAGATION_ITERATION_THRESHOLD = 16


# ============================================================
# Database Connection
# ============================================================

def establish_connection():
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 11 for SQL Server};"
            "SERVER=LOCALHOST\\HAMILTON;"
            "DATABASE=EvoYeast;"
            "UID=Hamilton;PWD=mkdpw:V43;Trust_Connection=no;"
        )
        return conn

    except Exception as e:
        log(f"ERROR: Failed to establish database connection: {e}")
        return None


# ============================================================
# Lookup Functions
# ============================================================

def get_latest_run_guid(cursor):
    try:
        cursor.execute(
            """
            SELECT TOP 1 RunGUID
            FROM HamiltonVectorDB.dbo.HxRun
            ORDER BY StartTime DESC
            """
        )

        row = cursor.fetchone()

        if not row:
            log("ERROR: No RunGUID found in HamiltonVectorDB.dbo.HxRun.")
            return None

        run_guid = row[0]
        log(f"Latest RunGUID retrieved: {run_guid}")

        return run_guid

    except Exception as e:
        log(f"ERROR: Failed to retrieve latest RunGUID: {e}")
        return None


def get_active_experiment_and_ancestor_plate(cursor):
    """
    Returns:
        (experiment_id, ancestor_plate_id)

    On failure:
        (None, None)
    """

    try:
        cursor.execute(
            """
            SELECT
                e.ExperimentID,
                api.PlateID AS AncestorPlateID
            FROM AncestPlatesInExperiments api
            INNER JOIN Experiments e
                ON api.ExperimentID = e.ExperimentID
            WHERE e.ScheduledToRun = 1
            """
        )

        rows = cursor.fetchall()

        if not rows:
            log(
                "ERROR: No active experiment found. "
                "Expected one experiment with Experiments.ScheduledToRun = 1."
            )
            return None, None

        if len(rows) > 1:
            details = "; ".join(
                f"ExperimentID={r[0]}, AncestorPlateID={r[1]}" for r in rows
            )
            log(
                "ERROR: Multiple active experiments or ancestor plates found. "
                f"Expected exactly one active ancestor plate. Found: {details}"
            )
            return None, None

        experiment_id = int(rows[0][0])
        ancestor_plate_id = int(rows[0][1])

        log(
            "Active experiment resolved: "
            f"ExperimentID={experiment_id}, "
            f"AncestorPlateID={ancestor_plate_id}"
        )

        return experiment_id, ancestor_plate_id

    except Exception as e:
        log(f"ERROR: Failed to resolve active experiment and ancestor plate: {e}")
        return None, None


def get_all_non_discarded_experiment_plates(cursor, ancestor_plate_id):
    """
    Returns:
        list of plate candidate dictionaries

    On failure:
        None
    """

    try:
        cursor.execute(
            """
            SELECT DISTINCT
                p.PlateID,
                p.BarCode,
                ISNULL(c.CytomatPos, -1) AS CytomatPos
            FROM Plates p
            LEFT OUTER JOIN Cytomat c
                ON p.PlateID = c.PlateID
            WHERE
                p.Discarded = 0
                AND (
                    p.PlateID = ?
                    OR p.PlateID IN (
                        SELECT DescPlateID
                        FROM dbo.Descendants(?)
                    )
                )
            ORDER BY p.PlateID DESC
            """,
            (ancestor_plate_id, ancestor_plate_id),
        )

        rows = cursor.fetchall()

        if not rows:
            log(
                "ERROR: No non-discarded plates found for "
                f"AncestorPlateID={ancestor_plate_id}."
            )
            return None

        candidates = []

        for row in rows:
            candidates.append(
                {
                    "plate_id": int(row[0]),
                    "barcode": row[1],
                    "cytomat_pos": row[2],
                }
            )

        log("Candidate plates belonging to active experiment:")

        for c in candidates:
            log(
                f"  PlateID={c['plate_id']}, "
                f"BarCode={c['barcode']}, "
                f"CytomatPos={c['cytomat_pos']}"
            )

        return candidates

    except Exception as e:
        log(f"ERROR: Failed to retrieve candidate experiment plates: {e}")
        return None


def select_plate_with_largest_plate_id(candidates):
    """
    Returns:
        selected plate dictionary

    On failure:
        None
    """

    try:
        if not candidates:
            log("ERROR: No candidate plates provided for current-plate selection.")
            return None

        selected = max(candidates, key=lambda x: x["plate_id"])

        log(
            "Selected current plate by largest PlateID: "
            f"PlateID={selected['plate_id']}, "
            f"BarCode={selected['barcode']}, "
            f"CytomatPos={selected['cytomat_pos']}"
        )

        return selected

    except Exception as e:
        log(f"ERROR: Failed to select current plate: {e}")
        return None


def get_latest_iteration_for_plate(cursor, plate_id):
    """
    Checks the latest reading already inserted into ChampionsCulturesHistory
    for cultures belonging to the selected plate.

    Returns:
        latest_iteration as int

    On failure:
        None
    """

    try:
        cursor.execute(
            """
            SELECT MAX(cch.Iteration) AS LatestIteration
            FROM dbo.ChampionsCulturesHistory cch
            INNER JOIN dbo.Cultures c
                ON cch.CultureID = c.CultureID
            WHERE c.PlateID = ?
            """,
            (plate_id,),
        )

        row = cursor.fetchone()

        if row is None or row[0] is None:
            log(
                "ERROR: No ChampionsCulturesHistory readings found for "
                f"PlateID={plate_id}."
            )
            return None

        latest_iteration = int(row[0])

        log(
            "Latest ChampionsCulturesHistory iteration for "
            f"PlateID={plate_id}: {latest_iteration}"
        )

        return latest_iteration

    except Exception as e:
        log(
            "ERROR: Failed to retrieve latest ChampionsCulturesHistory "
            f"iteration for PlateID={plate_id}: {e}"
        )
        return None


# ============================================================
# Output Function
# ============================================================

def write_propagation_decision_file(run_guid, decision_value):
    """
    Writes:
        C:\\EvoTaskFiles\\<RunGUID>_PropagationDecision.txt

    File content:
        1 = propagate
        0 = do not propagate

    This uses a temporary file first, then atomically replaces the final file.
    This prevents the final PropagationDecision file from being left empty.
    """

    try:
        # Validate BEFORE opening or truncating the final output file.
        if decision_value not in (0, 1):
            log(
                "ERROR: Invalid propagation decision value. "
                f"Expected 0 or 1, got: {decision_value}"
            )
            return None

        os.makedirs(EVOTASK_DIR, exist_ok=True)

        output_path = os.path.join(
            EVOTASK_DIR,
            f"{run_guid}_{OUTPUT_SUFFIX}.txt"
        )

        temp_path = output_path + ".tmp"

        # Include newline for more reliable reading by external tools.
        content = f"{int(decision_value)}\n"

        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())

        os.replace(temp_path, output_path)

        log(
            "Propagation decision file written successfully: "
            f"{output_path}; content={int(decision_value)}"
        )

        return output_path

    except Exception as e:
        log(f"ERROR: Failed to write propagation decision file: {e}")

        try:
            if "temp_path" in locals() and os.path.exists(temp_path):
                os.remove(temp_path)
                log(f"Temporary file removed after failure: {temp_path}")
        except Exception as cleanup_error:
            log(f"WARNING: Failed to remove temporary file: {cleanup_error}")

        return None


# ============================================================
# Main
# ============================================================

def main():
    conn = None

    log("=== Starting propagation-decision gatekeeper script ===")

    try:
        conn = establish_connection()

        if conn is None:
            log("=== Script failed: database connection could not be established ===")
            return 1

        cursor = conn.cursor()

        run_guid = get_latest_run_guid(cursor)

        if run_guid is None:
            log("=== Script failed: could not retrieve latest RunGUID ===")
            return 1

        experiment_id, ancestor_plate_id = get_active_experiment_and_ancestor_plate(
            cursor
        )

        if experiment_id is None or ancestor_plate_id is None:
            log("=== Script failed: could not resolve active experiment ===")
            return 1

        candidates = get_all_non_discarded_experiment_plates(
            cursor,
            ancestor_plate_id
        )

        if candidates is None:
            log("=== Script failed: could not retrieve experiment plates ===")
            return 1

        selected_plate = select_plate_with_largest_plate_id(candidates)

        if selected_plate is None:
            log("=== Script failed: could not select current plate ===")
            return 1

        selected_plate_id = selected_plate["plate_id"]
        selected_barcode = selected_plate["barcode"]
        selected_cytomat_pos = selected_plate["cytomat_pos"]

        latest_iteration = get_latest_iteration_for_plate(
            cursor,
            selected_plate_id
        )

        if latest_iteration is None:
            log("=== Script failed: could not retrieve latest iteration ===")
            return 1

        if latest_iteration >= PROPAGATION_ITERATION_THRESHOLD:
            propagation_decision = 1
        else:
            propagation_decision = 0

        log(
            "Decision calculated: "
            f"LatestIteration={latest_iteration}, "
            f"Threshold={PROPAGATION_ITERATION_THRESHOLD}, "
            f"PropagationDecision={propagation_decision}"
        )

        output_path = write_propagation_decision_file(
            run_guid,
            propagation_decision
        )

        if output_path is None:
            log("=== Script failed: could not write decision file ===")
            return 1

        log(
            "Final decision summary: "
            f"ExperimentID={experiment_id}, "
            f"AncestorPlateID={ancestor_plate_id}, "
            f"SelectedPlateID={selected_plate_id}, "
            f"SelectedBarCode={selected_barcode}, "
            f"CytomatPos={selected_cytomat_pos}, "
            f"LatestIteration={latest_iteration}, "
            f"Threshold={PROPAGATION_ITERATION_THRESHOLD}, "
            f"PropagationDecision={propagation_decision}, "
            f"OutputPath={output_path}"
        )

        log("=== Propagation-decision gatekeeper script completed successfully ===")
        return 0

    except Exception as e:
        log(f"FATAL ERROR: Unexpected exception reached main safety net: {e}")
        return 1

    finally:
        try:
            if conn is not None:
                conn.close()
                log("Database connection closed.")
        except Exception as e:
            log(f"WARNING: Failed to close database connection cleanly: {e}")


if __name__ == "__main__":
    sys.exit(main())