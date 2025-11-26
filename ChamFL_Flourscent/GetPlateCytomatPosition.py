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
    with open(log_file, "a") as f:
        f.write(f"[{now}] {msg}\n")

def establish_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 11 for SQL Server};"
        "SERVER=LOCALHOST\\HAMILTON;"
        "DATABASE=EvoYeast;"
        "UID=Hamilton;PWD=mkdpw:V43;Trust_Connection=no;"
    )

conn = establish_connection()
cursor = conn.cursor()
try:
    cursor.execute("EXEC dbo.Evo_RetrievePlateChain")
    rows1 = cursor.fetchall()
    cursor.nextset()
    rows2 = cursor.fetchall()
    if rows1 and rows2:
        # print("Handling rows1 + rows2")
        # print(rows1,rows2)
        barcode = rows2[0][0]
        # print(barcode)
        cursor.execute(f"SELECT PlateID from Plates WHERE BarCode = {barcode}")
        plateID_descendant = cursor.fetchall()[0][0]
        # print(plateID_descendant)
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
        if iteration_stage == 'FIRST':
            log("Detected Champions_FL Running In First Iteration")
            Cytomat_Position = rows1[0][1]
            barcode = rows1[0][0]
            log(f"Using Initial Parent Plate with Cytomat Position {Cytomat_Position}")
        else:
            log("Detected Champions_FL Running In Second Iteration")
            Cytomat_Position = rows2[0][1]  
            barcode = rows2[0][0]
            log(f"Using Initial Child Plate with Cytomat Position {Cytomat_Position}")
    elif not rows1:
        log("Detected Champions_FL Running In Third and Onward Iteration")
        Cytomat_Position = rows2[1][1]
        barcode = rows2[1][0]
        log(f"Using New Child Plate with Cytomat Position {Cytomat_Position}")

    cursor.execute("SELECT TOP 1 RunGUID FROM HamiltonVectorDB.dbo.HxRun ORDER BY StartTime DESC")
    run_id = cursor.fetchone()[0]
    log(f"Latest RunGUID retrieved: {run_id}")
    CytoPos_path = f"C:\\EvoTaskFiles\\{run_id}_CytomatPos.txt"
    with open(CytoPos_path, "w") as f:
        f.write(str(Cytomat_Position))
    log(f"Plate {barcode} with Cytomat Position {Cytomat_Position} written to {CytoPos_path}")
except Exception as e:
    log(f"Fatal error: {e}")
    sys.exit(1)
finally:
    conn.close()
    sys.exit(0)