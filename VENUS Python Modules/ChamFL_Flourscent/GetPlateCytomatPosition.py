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

def log(msg):
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
    return row[0]

def get_barcode_from_plate_id(cursor, plate_id):
    cursor.execute("SELECT BarCode FROM Plates WHERE PlateID = ?", (int(plate_id),))
    row = cursor.fetchone()
    if not row:
        raise RuntimeError(f"No BarCode found in Plates for PlateID={plate_id}")
    return row[0]

conn = establish_connection()
cursor = conn.cursor()

try:
    cursor.execute("EXEC dbo.Evo_RetrievePlateChain")
    rows1 = cursor.fetchall()
    cursor.nextset()
    rows2 = cursor.fetchall()

    Cytomat_Position = None
    barcode_selected = None

    if rows1 and rows2:
        # Use descendant plate to determine stage (as your original logic)
        barcode_descendant = rows2[0][0]
        plateID_descendant = get_plate_id_from_barcode(cursor, barcode_descendant)

        cursor.execute("""
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 FROM Cultures WHERE PlateID = ? AND WellID IS NOT NULL
                )
                THEN 'SECOND'
                ELSE 'FIRST'
            END AS IterationStage
        """, (plateID_descendant,))
        iteration_stage = cursor.fetchone()[0]

        if iteration_stage == "FIRST":
            log("Detected Champions_FL Running In First Iteration")
            Cytomat_Position = rows1[0][1]
            barcode_selected = rows1[0][0]
            log(f"Using Initial Parent Plate with Cytomat Position {Cytomat_Position}")
        else:
            log("Detected Champions_FL Running In Second Iteration")
            Cytomat_Position = rows2[0][1]
            barcode_selected = rows2[0][0]
            log(f"Using Initial Child Plate with Cytomat Position {Cytomat_Position}")

    elif not rows1:
        log("Detected Champions_FL Running In Third and Onward Iteration")
        Cytomat_Position = rows2[1][1]
        barcode_selected = rows2[1][0]
        log(f"Using New Child Plate with Cytomat Position {Cytomat_Position}")

    if barcode_selected is None or Cytomat_Position is None:
        raise RuntimeError("Could not determine barcode/Cytomat position from Evo_RetrievePlateChain output.")

    # You said: output barcode by searching Plates with PlateID.
    # So: resolve PlateID for the selected barcode, then re-fetch barcode via PlateID.
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

    CytoPos_path = os.path.join(evotask_dir, f"{run_id}_CytomatPos.txt")
    Barcode_path = os.path.join(evotask_dir, f"{run_id}_PlateBarcode.txt")

    with open(CytoPos_path, "w", encoding="utf-8") as f:
        f.write(str(Cytomat_Position))
    log(f"Cytomat position {Cytomat_Position} written to {CytoPos_path}")

    with open(Barcode_path, "w", encoding="utf-8") as f:
        f.write(str(barcode_via_plateid))
    log(f"Plate barcode {barcode_via_plateid} written to {Barcode_path}")

    log(f"Plate {barcode_via_plateid} selected with Cytomat Position {Cytomat_Position}")

except Exception as e:
    log(f"Fatal error: {e}")
    try:
        conn.close()
    except Exception:
        pass
    sys.exit(1)
finally:
    try:
        conn.close()
    except Exception:
        pass
    sys.exit(0)