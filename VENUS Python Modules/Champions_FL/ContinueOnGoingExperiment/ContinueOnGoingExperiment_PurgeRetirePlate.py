import pyodbc
import os
import sys
import random
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
    server = 'LOCALHOST\\HAMILTON'
    db = 'EvoYeast'
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 11 for SQL Server}};"
        f"SERVER={server};DATABASE={db};UID=Hamilton;PWD=mkdpw:V43;Trust_Connection=no;"
    )

try:
    log("=== Script started ===")
    conn = establish_connection()
    cursor = conn.cursor()
    log("Database connection established.")

    # === Get latest RunGUID ===
    cursor.execute("SELECT TOP 1 RunGUID FROM HamiltonVectorDB.dbo.HxRun ORDER BY StartTime DESC")
    row = cursor.fetchone()
    if not row:
        log("ERROR: No RunGUID found. Exiting.")
        sys.exit(1)
    run_id = row[0]
    log(f"Latest RunGUID: {run_id}")

    # === Step 1: Get ancestor PlateID ===
    log("Retrieving ancestor PlateID...")
    cursor.execute("""
        SELECT AncestPlatesInExperiments.PlateID
        FROM AncestPlatesInExperiments
        INNER JOIN Experiments ON AncestPlatesInExperiments.ExperimentID = Experiments.ExperimentID
        WHERE Experiments.ScheduledToRun = 1
    """)
    row = cursor.fetchone()
    if not row:
        log("ERROR: No ancestor plate found. Exiting.")
        sys.exit(1)
    pid = row[0]
    log(f"Ancestor PlateID found: {pid}")

    # === Step 2: Get descendant PlateIDs ===
    log("Retrieving descendant plates...")
    cursor.execute("""
        SELECT DISTINCT Plates.PlateID
        FROM Plates
        INNER JOIN dbo.Descendants(?) AS D ON Plates.PlateID = D.DescPlateID
    """, pid)
    descendants = [r[0] for r in cursor.fetchall()]
    log(f"Retrieved {len(descendants)} descendant plates.")

    # === Step 3: Build full plate chain ===
    plate_chain = [pid] + descendants
    placeholders = ",".join("?" * len(plate_chain))

    # === Step 4: Retrieve existing barcodes from these plates ===
    log("Retrieving barcodes for the plate chain...")
    cursor.execute(f"SELECT BarCode FROM Plates WHERE PlateID IN ({placeholders})", plate_chain)
    existing_barcodes = {str(r[0]) for r in cursor.fetchall() if r[0]}
    log(f"Collected {len(existing_barcodes)} barcodes in plate chain.")

    # === Step 5: Generate a new unique barcode ===
    def gen_unique_bc(existing):
        while True:
            bc = f"BC{random.randint(1000, 9999)}"
            if bc not in existing:
                return bc

    new_bc = gen_unique_bc(existing_barcodes)
    log(f"Generated new barcode: {new_bc}")

    # === Step 6: Add the new plate ===
    log(f"Calling AddExpansionPlateToActiveExperiment with barcode {new_bc}...")
    try:
        cursor.execute("EXEC dbo.AddExpansionPlateToActiveExperiment @Barcode = ?", (new_bc,))

        # ✅ Fetch any result returned by the stored procedure
        sp_result = cursor.fetchall()
        conn.commit()
        log("Stored procedure executed and committed successfully.")

    except Exception as e:
        log(f"ERROR executing AddExpansionPlateToActiveExperiment: {e}")
        sys.exit(1)

    # === Step 7: Write the result file with SP output ===
    task_dir = r"C:\EvoTaskFiles"
    os.makedirs(task_dir, exist_ok=True)
    result_path = os.path.join(task_dir, f"{run_id}_AddPlate.txt")

    try:
        with open(result_path, "w") as f:
            if sp_result:
                # Convert each row to comma-separated text
                for row in sp_result:
                    f.write(",".join([str(x) if x is not None else "" for x in row]) + "\n")
                log(f"Result file written with {len(sp_result)} rows: {result_path}")
            else:
                # If no result set, still log and write the barcode for traceability
                f.write(new_bc)
                log(f"Stored procedure returned no data; only barcode written to {result_path}")
    except Exception as file_e:
        log(f"ERROR writing result file: {file_e}")
        sys.exit(1)

    conn.close()
    log("=== Script completed successfully ===")
    sys.exit(0)


except Exception as e:
    log(f"Fatal error: {e}")
    sys.exit(1)
