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

try:
    log("=== Script started ===")
    conn = establish_connection()
    cursor = conn.cursor()
    log("Database connection established successfully.")

    # === Get latest RunGUID ===
    log("Fetching latest RunGUID...")
    cursor.execute("SELECT TOP 1 RunGUID FROM HamiltonVectorDB.dbo.HxRun ORDER BY StartTime DESC")
    run_id = cursor.fetchone()[0]
    log(f"Latest RunGUID retrieved: {run_id}")

    # === PlateChain Check ===
    PlateChain_path = f"C:\\EvoTaskFiles\\{run_id}_PlateChainChecked.txt"
    log("Retrieving plate chain using Evo_RetrievePlateChain...")

    checked_chains = []
    try:
        cursor.execute("EXEC dbo.Evo_RetrievePlateChain")

        # First result set (ancestor)
        rows1 = cursor.fetchall()
        log(f"Retrieved {len(rows1)} rows from the first result set (ancestor).")
        checked_chains += [[str(r[0]), str(r[1])] for r in rows1 if r and r[0]]

        # Second result set (descendants)
        if cursor.nextset():
            rows2 = cursor.fetchall()
            log(f"Retrieved {len(rows2)} rows from the second result set (descendants).")
            checked_chains += [[str(r[0]), str(r[1])] for r in rows2 if r and r[0]]
        else:
            log("No second result set returned.")

        if not checked_chains:
            log("ERROR: No plates found in chain. Exiting.")
            sys.exit(1)

    except pyodbc.ProgrammingError:
        log("ERROR: Evo_RetrievePlateChain returned no valid result sets.")
        sys.exit(1)
    except Exception as e:
        log(f"ERROR retrieving plate chain: {e}")
        sys.exit(1)

    # === Validate each plate ===
    valid_chains = []
    for bc, pos in checked_chains:
        try:
            log(f"Validating plate {bc} (Cytomat {pos})...")
            cursor.execute("EXEC dbo.Competition_SelectCultures @Barcode = ?, @RunID = ?", [bc, run_id])
            result = cursor.fetchall()
            if result:
                log(f"Plate {bc} is valid.")
                valid_chains.append([bc, pos])
            else:
                log(f"Plate {bc} has no culture data, skipping.")
        except Exception as e:
            log(f"ERROR validating plate {bc}: {e}")
            sys.exit(1)

    # === Write final PlateChainChecked.txt ===
    try:
        if valid_chains:
            with open(PlateChain_path, "w") as f:
                f.write("\n".join([",".join(item) for item in valid_chains]))
            log(f"PlateChainChecked file written: {PlateChain_path}")
        else:
            log("No valid plates found, file not created.")
            sys.exit(1)
    except Exception as e:
        log(f"ERROR writing PlateChainChecked file: {e}")
        sys.exit(1)

    conn.close()
    log("=== Script completed successfully ===")
    sys.exit(0)

except Exception as e:
    log(f"Fatal error: {e}")
    sys.exit(1)
