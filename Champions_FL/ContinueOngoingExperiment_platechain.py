import pyodbc
import sys
import argparse
import csv
import os
import subprocess
from datetime import datetime

# === Setup dynamic log file ===
log_dir = r"C:\Python Log"
os.makedirs(log_dir, exist_ok=True)

script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(log_dir, f"{script_name}_{timestamp}.log")

def log(message):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{now}] {message}\n")

# === Parse arguments ===
parser = argparse.ArgumentParser()
parser.add_argument("PlateChainBarcode", type=str, help="PlateChainBarcode identifier")
parser.add_argument("SpatialEvoPlateID", type=str, help="SpatialEvoPlateID identifier")
parser.add_argument("SpillOverPlateID", type=str, help="SpillOverPlateID identifier")
args = parser.parse_args()

PlateChainBarcode = args.PlateChainBarcode
SpatialEvoPlate = args.SpatialEvoPlateID
SpillOverPlate = args.SpillOverPlateID

# === Constants for Hamilton formatting ===
LAYOUT_PATH = r"C:\PROGRAM FILES\HAMILTON\METHODS\LABPROTOCOLS\EXPERIMENTS\DECKS\SPATIALEVOLUTION3OD384WELL.LAY"

def establish_connection():
    server_name = 'LOCALHOST\\HAMILTON'
    database_name = 'EvoYeast'
    username = 'Hamilton'
    password = 'mkdpw:V43'
    connection_string = (
        f"DRIVER={{ODBC Driver 11 for SQL Server}};"
        f"SERVER={server_name};"
        f"DATABASE={database_name};"
        f"UID={username};"
        f"PWD={password};"
        f"Trust_Connection=no;"
    )
    return pyodbc.connect(connection_string)

try:
    log("=== Script started ===")
    conn = establish_connection()
    log("Database connection established successfully.")
    cursor = conn.cursor()

    # === Get latest RunGUID ===
    log("Fetching latest RunGUID...")
    cursor.execute("SELECT TOP 1 RunGUID FROM HamiltonVectorDB.dbo.HxRun ORDER BY StartTime DESC")
    run_id = cursor.fetchone()[0]
    log(f"Latest RunGUID retrieved: {run_id}")

    # === Call stored procedure: Competition_SelectCultures ===
    log("Executing stored procedure: Competition_SelectCultures")
    try:
        cursor.execute("EXEC dbo.Competition_SelectCultures @Barcode = ?, @RunID = ?", [PlateChainBarcode, run_id])
        culture_result = cursor.fetchall()
        if not culture_result:
            log("No data returned from Competition_SelectCultures. Exiting with code 1.")
            sys.exit(1)

        log(f"Retrieved {len(culture_result)} rows from Competition_SelectCultures.")
        Output_Temp_ImportSpatialEvoODSubset_Path = f"C:\\EvoTaskFiles\\{run_id}_subset.txt"

        generated_rows = [[row[0], run_id, row[1], 1] for row in culture_result]
        log("Writing temporary subset file for BCP import...")
        with open(Output_Temp_ImportSpatialEvoODSubset_Path, "w", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerows(generated_rows)
        log(f"Subset file written: {Output_Temp_ImportSpatialEvoODSubset_Path}")

        # === Run BCP import (hidden window) ===
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW  # ✅ hide the console window

        bcp_command = [
            "bcp",
            "EvoYeast.dbo.ImportSpatialEvoODSubset",
            "in", Output_Temp_ImportSpatialEvoODSubset_Path,
            "-T", "-c",
            "-S", "HAMILTON-PC\\HAMILTON"
        ]

        log("Starting BCP import (hidden window)...")
        result = subprocess.run(bcp_command, capture_output=True, text=True, startupinfo=startupinfo)
        if result.returncode != 0:
            log(f"BCP failed with error: {result.stderr}")
            sys.exit(1)
        log("BCP import completed successfully.")

    except Exception as e:
        log(f"ERROR during Competition_SelectCultures or BCP phase: {e}")
        sys.exit(1)

    # === Extract experiment parameters ===
    log("Loading experiment parameters...")
    parameters = ["TargetWellVolume", "InoculationOD", "TopFractionToPropagate", "V_OD_Sample"]
    experiment_params = {}

    for param in parameters:
        try:
            cursor.execute("SELECT dbo.ReadExperimentParameter(NULL, ?)", (param,))
            res = cursor.fetchone()
            if not res or res[0] is None:
                log(f"ERROR: Parameter {param} missing.")
                sys.exit(1)
            experiment_params[param] = res[0]
            log(f"Parameter loaded: {param} = {res[0]}")
        except Exception as e:
            log(f"ERROR retrieving parameter {param}: {e}")
            sys.exit(1)

    # === Call Champions_CommencePropagationFl ===
    log("Executing Champions_CommencePropagationFl stored procedure...")
    try:
        cursor.execute(
            "EXEC dbo.Champions_CommencePropagationFl @TargetVol=?, @RunId=?, @InoculationOD=?, @TopFractionToPropagate=?, @ODSampleVol=?",
            (experiment_params["TargetWellVolume"], run_id, experiment_params["InoculationOD"],
             experiment_params["TopFractionToPropagate"], experiment_params["V_OD_Sample"])
        )
        selection_result = cursor.fetchall()
        conn.commit()  # ✅ ensure DB inserts from SP persist
    except Exception as e:
        log(f"ERROR executing Champions_CommencePropagationFl: {e}")
        sys.exit(1)

    if not selection_result:
        log("No data returned from Champions_CommencePropagationFl. Exiting with code 1.")
        sys.exit(1)

    log(f"Retrieved {len(selection_result)} propagation records. Generating output files...")

    # === Prepare data ===
    cytomat_pos = [str(row[0]) for row in selection_result if row[0] is not None]
    spill_positions = [row[2] for row in selection_result]
    well_positions = [row[3] for row in selection_result]
    culture_vol = [str(row[4]) for row in selection_result]
    media_vol = [str(row[5]) for row in selection_result]

    # Define file paths
    CytoPos_path = f"C:\\EvoTaskFiles\\{run_id}_CytomatPos.txt"
    CultureVol_path = f"C:\\EvoTaskFiles\\{run_id}_CultureVol.txt"
    MediaVol_path = f"C:\\EvoTaskFiles\\{run_id}_MediaVol.txt"
    SpillOverPlateSeq_path = f"C:\\EvoTaskFiles\\{run_id}_SpillOverPlate_Positions.txt"
    SpatialOverPlateSeq_path = f"C:\\EvoTaskFiles\\{run_id}_SpatialOverPlate_Positions.txt"

    # Write CytomatPos
    log("Writing CytomatPos.txt...")
    with open(CytoPos_path, "w") as f:
        f.write("\n".join(cytomat_pos))

    # Helper for Hamilton-format files
    def write_hamilton_format(filename, positions, labware, sequence):
        with open(filename, "w", newline="") as f:
            f.write('Id,Layout,Sequence,Labware,Position\n')
            for i, pos in enumerate(positions, 1):
                f.write(f'{i},{LAYOUT_PATH},{sequence},{labware},{pos}\n')
        log(f"File generated: {filename}")

    # Write Hamilton-formatted files
    write_hamilton_format(SpillOverPlateSeq_path, spill_positions, SpillOverPlate, "seqSpillOverPlate")
    write_hamilton_format(SpatialOverPlateSeq_path, well_positions, SpatialEvoPlate, "seqEvoSrcPlate")

    # Write volumes
    log("Writing CultureVol.txt...")
    with open(CultureVol_path, "w") as f:
        f.write("\n".join(culture_vol))

    log("Writing MediaVol.txt...")
    with open(MediaVol_path, "w") as f:
        f.write("\n".join(media_vol))

    # Close connection
    conn.close()
    log("All files generated successfully. === Script completed ===")
    sys.exit(0)

except Exception as e:
    log(f"Fatal error: {e}")
    sys.exit(1)
