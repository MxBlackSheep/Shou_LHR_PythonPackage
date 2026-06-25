import pyodbc
import sys
import os
from datetime import datetime


# === Setup logging ===
log_dir = r"C:\Python Log"
os.makedirs(log_dir, exist_ok=True)

script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(log_dir, f"{script_name}_{timestamp}.log")


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {msg}\n")


def establish_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 11 for SQL Server};"
        "SERVER=LOCALHOST\\HAMILTON;"
        "DATABASE=EvoYeast;"
        "UID=Hamilton;PWD=mkdpw:V43;Trust_Connection=no;"
    )


def get_latest_run_guid(cursor):
    cursor.execute(
        """
        SELECT TOP 1 RunGUID
        FROM HamiltonVectorDB.dbo.HxRun
        ORDER BY StartTime DESC
        """
    )
    row = cursor.fetchone()

    if not row:
        raise RuntimeError("No RunGUID found in HamiltonVectorDB.dbo.HxRun")

    return row[0]


def get_active_experiment_and_ancestor_plate(cursor):
    """
    Finds the active experiment and its ancestor plate.

    This replaces the previous dependence on the stored procedure output.
    The active experiment is defined by Experiments.ScheduledToRun = 1.
    """

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
        raise RuntimeError(
            "No active experiment found. "
            "Expected one experiment with Experiments.ScheduledToRun = 1."
        )

    if len(rows) > 1:
        details = "; ".join(
            f"ExperimentID={r[0]}, AncestorPlateID={r[1]}" for r in rows
        )
        raise RuntimeError(
            "Multiple active experiments or ancestor plates found. "
            f"Expected exactly one active ancestor plate. Found: {details}"
        )

    experiment_id = int(rows[0][0])
    ancestor_plate_id = int(rows[0][1])

    log(
        f"Active experiment resolved: "
        f"ExperimentID={experiment_id}, AncestorPlateID={ancestor_plate_id}"
    )

    return experiment_id, ancestor_plate_id


def get_all_non_discarded_experiment_plates(cursor, ancestor_plate_id):
    """
    Retrieves the ancestor plate plus all descendant plates, then excludes discarded plates.

    Candidate plates are:
        - the ancestor plate
        - every DescPlateID returned by dbo.Descendants(ancestor_plate_id)

    The selected plate will later be the candidate with the largest PlateID.
    """

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
        raise RuntimeError(
            f"No non-discarded plates found for ancestor PlateID={ancestor_plate_id}"
        )

    candidates = []
    for row in rows:
        plate_id = int(row[0])
        barcode = row[1]
        cytomat_pos = row[2]

        candidates.append(
            {
                "plate_id": plate_id,
                "barcode": barcode,
                "cytomat_pos": cytomat_pos,
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


def select_plate_with_largest_plate_id(candidates):
    """
    Selects the latest/current plate by largest PlateID.

    This works better for the new two-plate cycling context because it does not
    rely on interpreting whether the run is first, second, or later iteration.
    """

    if not candidates:
        raise RuntimeError("No candidate plates provided for selection.")

    selected = max(candidates, key=lambda x: x["plate_id"])

    log(
        "Selected plate with largest PlateID: "
        f"PlateID={selected['plate_id']}, "
        f"BarCode={selected['barcode']}, "
        f"CytomatPos={selected['cytomat_pos']}"
    )

    return selected


def write_output_files(run_id, cytomat_position, barcode):
    evotask_dir = r"C:\EvoTaskFiles"
    os.makedirs(evotask_dir, exist_ok=True)

    cyto_pos_path = os.path.join(evotask_dir, f"{run_id}_CytomatPos.txt")
    barcode_path = os.path.join(evotask_dir, f"{run_id}_PlateBarcode.txt")

    with open(cyto_pos_path, "w", encoding="utf-8") as f:
        f.write(str(cytomat_position))

    log(f"Cytomat position {cytomat_position} written to {cyto_pos_path}")

    with open(barcode_path, "w", encoding="utf-8") as f:
        f.write(str(barcode))

    log(f"Plate barcode {barcode} written to {barcode_path}")


def main():
    conn = None

    try:
        log("=== Starting active-experiment plate selection script ===")

        conn = establish_connection()
        cursor = conn.cursor()

        experiment_id, ancestor_plate_id = get_active_experiment_and_ancestor_plate(cursor)

        candidates = get_all_non_discarded_experiment_plates(
            cursor,
            ancestor_plate_id,
        )

        selected = select_plate_with_largest_plate_id(candidates)

        selected_plate_id = selected["plate_id"]
        selected_barcode = selected["barcode"]
        selected_cytomat_position = selected["cytomat_pos"]

        run_id = get_latest_run_guid(cursor)
        log(f"Latest RunGUID retrieved: {run_id}")

        write_output_files(
            run_id=run_id,
            cytomat_position=selected_cytomat_position,
            barcode=selected_barcode,
        )

        log(
            "Final selected plate: "
            f"ExperimentID={experiment_id}, "
            f"AncestorPlateID={ancestor_plate_id}, "
            f"SelectedPlateID={selected_plate_id}, "
            f"SelectedBarCode={selected_barcode}, "
            f"CytomatPos={selected_cytomat_position}"
        )

        log("=== Plate selection script completed successfully ===")
        return 0

    except Exception as e:
        log(f"Fatal error: {e}")
        return 1

    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())