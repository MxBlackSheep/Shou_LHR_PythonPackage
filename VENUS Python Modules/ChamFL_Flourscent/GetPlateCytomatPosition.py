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


def get_plate_id_from_barcode(cursor, barcode_value):
    cursor.execute("SELECT PlateID FROM Plates WHERE BarCode = ?", (str(barcode_value),))
    row = cursor.fetchone()
    if not row:
        raise RuntimeError(f"No PlateID found in Plates for BarCode={barcode_value}")
    return int(row[0])


def get_barcode_from_plate_id(cursor, plate_id):
    cursor.execute("SELECT BarCode FROM Plates WHERE PlateID = ?", (int(plate_id),))
    row = cursor.fetchone()
    if not row:
        raise RuntimeError(f"No BarCode found in Plates for PlateID={plate_id}")
    return row[0]


def pick_row_with_largest_plate_id(cursor, candidate_rows):
    """
    candidate_rows: list of rows where row[0]=barcode, row[1]=cytomat_position
    Returns: (barcode, cytomat_position, plate_id)
    """
    if not candidate_rows:
        raise RuntimeError("No candidate rows provided to pick_row_with_largest_plate_id")

    scored = []
    for r in candidate_rows:
        bc = r[0]
        cyto = r[1]
        pid = get_plate_id_from_barcode(cursor, bc)
        scored.append((pid, bc, cyto))

    scored.sort(key=lambda x: x[0], reverse=True)  # highest PlateID first
    best_pid, best_bc, best_cyto = scored[0]

    log("Case B candidate PlateIDs:")
    for pid, bc, cyto in scored:
        log(f"  Candidate BarCode={bc}, CytomatPos={cyto}, PlateID={pid}")
    log(f"Selected candidate with largest PlateID: BarCode={best_bc}, CytomatPos={best_cyto}, PlateID={best_pid}")

    return best_bc, best_cyto, best_pid


def main():
    conn = None
    try:
        conn = establish_connection()
        cursor = conn.cursor()

        cursor.execute("EXEC dbo.Evo_RetrievePlateChain")
        rows1 = cursor.fetchall()
        cursor.nextset()
        rows2 = cursor.fetchall()

        cytomat_position = None
        barcode_selected = None

        if rows1 and rows2:
            # Use descendant plate to determine stage
            barcode_descendant = rows2[0][0]
            plate_id_descendant = get_plate_id_from_barcode(cursor, barcode_descendant)

            cursor.execute(
                """
                SELECT CASE
                    WHEN EXISTS (
                        SELECT 1 FROM Cultures WHERE PlateID = ? AND WellID IS NOT NULL
                    )
                    THEN 'SECOND'
                    ELSE 'FIRST'
                END AS IterationStage
                """,
                (plate_id_descendant,),
            )
            iteration_stage = cursor.fetchone()[0]

            if iteration_stage == "FIRST":
                log("Detected Champions_FL Running In First Iteration")
                cytomat_position = rows1[0][1]
                barcode_selected = rows1[0][0]
                log(f"Using Initial Parent Plate with Cytomat Position {cytomat_position}")
            else:
                log("Detected Champions_FL Running In Second Iteration")
                cytomat_position = rows2[0][1]
                barcode_selected = rows2[0][0]
                log(f"Using Initial Child Plate with Cytomat Position {cytomat_position}")

        elif not rows1 and rows2:
            log("Detected Champions_FL Running In Third and Onward Iteration")

            # Updated Case B logic:
            # Choose whichever candidate in rows2 has the larger PlateID
            barcode_selected, cytomat_position, _ = pick_row_with_largest_plate_id(cursor, rows2)

            log(f"Using Child Plate selected by largest PlateID with Cytomat Position {cytomat_position}")

        else:
            raise RuntimeError("Unexpected output from Evo_RetrievePlateChain (no usable rows).")

        if barcode_selected is None or cytomat_position is None:
            raise RuntimeError("Could not determine barcode and Cytomat position from Evo_RetrievePlateChain output.")

        # Confirm barcode via PlateID (output must be barcode obtained from Plates using PlateID)
        plate_id = get_plate_id_from_barcode(cursor, barcode_selected)
        barcode_via_plateid = get_barcode_from_plate_id(cursor, plate_id)
        log(f"Resolved PlateID={plate_id} and confirmed BarCode={barcode_via_plateid} from Plates")

        cursor.execute("SELECT TOP 1 RunGUID FROM HamiltonVectorDB.dbo.HxRun ORDER BY StartTime DESC")
        run_id_row = cursor.fetchone()
        if not run_id_row:
            raise RuntimeError("No RunGUID found in HamiltonVectorDB.dbo.HxRun")
        run_id = run_id_row[0]
        log(f"Latest RunGUID retrieved: {run_id}")

        # Write outputs
        evotask_dir = r"C:\EvoTaskFiles"
        os.makedirs(evotask_dir, exist_ok=True)

        cyto_pos_path = os.path.join(evotask_dir, f"{run_id}_CytomatPos.txt")
        barcode_path = os.path.join(evotask_dir, f"{run_id}_PlateBarcode.txt")

        with open(cyto_pos_path, "w", encoding="utf-8") as f:
            f.write(str(cytomat_position))
        log(f"Cytomat position {cytomat_position} written to {cyto_pos_path}")

        with open(barcode_path, "w", encoding="utf-8") as f:
            f.write(str(barcode_via_plateid))
        log(f"Plate barcode {barcode_via_plateid} written to {barcode_path}")

        log(f"Plate {barcode_via_plateid} selected with Cytomat Position {cytomat_position}")
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