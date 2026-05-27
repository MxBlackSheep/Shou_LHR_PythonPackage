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


def log(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
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
LAYOUT_PATH = (
    r"C:\PROGRAM FILES\HAMILTON\METHODS\LABPROTOCOLS\EXPERIMENTS\DECKS"
    r"\SPATIALEVOLUTION3OD384WELL.LAY"
)


def establish_connection():
    server_name = r"LOCALHOST\HAMILTON"
    database_name = "EvoYeast"
    username = "Hamilton"
    password = "mkdpw:V43"

    connection_string = (
        "DRIVER={ODBC Driver 11 for SQL Server};"
        f"SERVER={server_name};"
        f"DATABASE={database_name};"
        f"UID={username};"
        f"PWD={password};"
        "Trust_Connection=no;"
    )

    return pyodbc.connect(connection_string)


def run_bcp_import(input_file_path: str) -> None:
    """
    Import the temporary subset file into EvoYeast.dbo.ImportSpatialEvoODSubset.
    """
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

    bcp_command = [
        "bcp",
        "EvoYeast.dbo.ImportSpatialEvoODSubset",
        "in",
        input_file_path,
        "-T",
        "-c",
        "-S",
        r"HAMILTON-PC\HAMILTON",
    ]

    log("Starting BCP import...")
    result = subprocess.run(
        bcp_command,
        capture_output=True,
        text=True,
        startupinfo=startupinfo,
    )

    if result.returncode != 0:
        log(f"BCP failed. Return code: {result.returncode}")
        log(f"BCP stdout: {result.stdout}")
        log(f"BCP stderr: {result.stderr}")
        raise RuntimeError(f"BCP import failed: {result.stderr}")

    log("BCP import completed successfully.")
    if result.stdout:
        log(f"BCP stdout: {result.stdout}")


def fetch_all_result_sets(cursor) -> list:
    """
    Read all result sets returned by a stored procedure.

    Champions_CommencePropagationFl returns one SELECT result inside each
    WHILE-loop iteration. For 50% propagation, that means two result sets.
    A plain cursor.fetchall() only reads the first one, which causes only
    8 rows to be seen when 16 rows are expected.
    """
    all_rows = []
    result_set_index = 0

    while True:
        result_set_index += 1

        try:
            rows = cursor.fetchall()
            log(f"Result set {result_set_index}: fetched {len(rows)} rows.")

            for row in rows:
                # Stored procedure ends with dummy row:
                # SELECT 0, null, null, null, 0, 0, 0
                #
                # Expected real output columns:
                # 0 CytomatPos
                # 1 SpilloverPlateBarcode
                # 2 SpillOverPlateWellID
                # 3 ToBePropagatedPlateWellID
                # 4 InocVol
                # 5 MediaVol
                # 6 SpilloverPlateID
                if len(row) >= 7:
                    is_dummy_row = (
                        row[0] == 0
                        and row[1] is None
                        and row[2] is None
                        and row[3] is None
                        and row[4] == 0
                        and row[5] == 0
                        and row[6] == 0
                    )

                    if is_dummy_row:
                        log("Dummy final row ignored.")
                        continue

                all_rows.append(row)

        except pyodbc.ProgrammingError as e:
            # Some result sets may contain no rows.
            log(f"Result set {result_set_index}: no fetchable rows. {e}")

        has_next = cursor.nextset()
        if not has_next:
            break

    log(f"Total rows collected across all result sets: {len(all_rows)}")
    return all_rows


def write_hamilton_format(filename: str, positions: list, labware: str, sequence: str) -> None:
    """
    Write Hamilton sequence file.
    """
    with open(filename, "w", newline="", encoding="utf-8") as f:
        f.write("Id,Layout,Sequence,Labware,Position\n")
        for i, pos in enumerate(positions, 1):
            f.write(f"{i},{LAYOUT_PATH},{sequence},{labware},{pos}\n")

    log(f"File generated: {filename}")


try:
    log("=== Script started ===")
    log(f"PlateChainBarcode: {PlateChainBarcode}")
    log(f"SpatialEvoPlate labware ID: {SpatialEvoPlate}")
    log(f"SpillOverPlate labware ID: {SpillOverPlate}")

    conn = establish_connection()
    log("Database connection established successfully.")
    cursor = conn.cursor()

    # === Get latest RunGUID ===
    log("Fetching latest RunGUID...")
    cursor.execute(
        "SELECT TOP 1 RunGUID "
        "FROM HamiltonVectorDB.dbo.HxRun "
        "ORDER BY StartTime DESC"
    )

    run_row = cursor.fetchone()
    if not run_row or not run_row[0]:
        log("ERROR: Could not retrieve latest RunGUID.")
        sys.exit(1)

    run_id = run_row[0]
    log(f"Latest RunGUID retrieved: {run_id}")

    # === Call stored procedure: Competition_SelectCultures ===
    log("Executing stored procedure: Competition_SelectCultures")

    try:
        cursor.execute(
            "EXEC dbo.Competition_SelectCultures @Barcode = ?, @RunID = ?",
            (PlateChainBarcode, run_id),
        )

        culture_result = cursor.fetchall()

        if not culture_result:
            log("No data returned from Competition_SelectCultures. Exiting with code 1.")
            sys.exit(1)

        log(f"Retrieved {len(culture_result)} rows from Competition_SelectCultures.")

        output_subset_path = f"C:\\EvoTaskFiles\\{run_id}_subset.txt"

        # Expected source SP output:
        # row[0] = PlateID
        # row[1] = WellID
        #
        # ImportSpatialEvoODSubset format:
        # PlateID, RunID, WellID, correction/flag value
        generated_rows = [[row[0], run_id, row[1], 1] for row in culture_result]

        log("Writing temporary subset file for BCP import...")
        with open(output_subset_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerows(generated_rows)

        log(f"Subset file written: {output_subset_path}")

        run_bcp_import(output_subset_path)

    except Exception as e:
        log(f"ERROR during Competition_SelectCultures or BCP phase: {e}")
        sys.exit(1)

    # === Extract experiment parameters ===
    log("Loading experiment parameters...")

    parameters = [
        "TargetWellVolume",
        "InoculationOD",
        "TopFractionToPropagate",
        "V_OD_Sample",
    ]

    experiment_params = {}

    for param in parameters:
        try:
            cursor.execute("SELECT dbo.ReadExperimentParameter(NULL, ?)", (param,))
            res = cursor.fetchone()

            if not res or res[0] is None:
                log(f"ERROR: Parameter {param} missing.")
                sys.exit(1)

            experiment_params[param] = float(res[0])
            log(f"Parameter loaded: {param} = {experiment_params[param]}")

        except Exception as e:
            log(f"ERROR retrieving parameter {param}: {e}")
            sys.exit(1)

    # === Estimate expected number of rows for logging ===
    try:
        top_fraction = experiment_params["TopFractionToPropagate"]
        candidate_count = len(culture_result)

        expected_selected = int(-(-candidate_count * top_fraction // 1))  # ceiling
        expected_split_count = int(1.0 // top_fraction)
        expected_rows = expected_selected * expected_split_count

        log(
            "Expected propagation estimate: "
            f"candidate_count={candidate_count}, "
            f"top_fraction={top_fraction}, "
            f"expected_selected={expected_selected}, "
            f"expected_split_count={expected_split_count}, "
            f"expected_rows_before_final_cap={expected_rows}"
        )

    except Exception as e:
        log(f"Could not calculate expected propagation estimate: {e}")

    # === Call Champions_CommencePropagationFl ===
    log("Executing Champions_CommencePropagationFl stored procedure...")

    try:
        cursor.execute(
            "EXEC dbo.Champions_CommencePropagationFl "
            "@TargetVol=?, "
            "@RunId=?, "
            "@InoculationOD=?, "
            "@TopFractionToPropagate=?, "
            "@ODSampleVol=?",
            (
                experiment_params["TargetWellVolume"],
                run_id,
                experiment_params["InoculationOD"],
                experiment_params["TopFractionToPropagate"],
                experiment_params["V_OD_Sample"],
            ),
        )

        # IMPORTANT:
        # Read all result sets, not only the first one.
        selection_result = fetch_all_result_sets(cursor)

        conn.commit()

    except Exception as e:
        log(f"ERROR executing Champions_CommencePropagationFl: {e}")
        try:
            conn.rollback()
            log("Transaction rolled back.")
        except Exception:
            pass
        sys.exit(1)

    if not selection_result:
        log("No usable propagation data returned from Champions_CommencePropagationFl. Exiting with code 1.")
        sys.exit(1)

    log(f"Retrieved total {len(selection_result)} propagation records. Generating output files...")

    # === Prepare data ===
    try:
        cytomat_pos = [str(row[0]) for row in selection_result if row[0] is not None]
        spill_positions = [str(row[2]) for row in selection_result]
        well_positions = [str(row[3]) for row in selection_result]
        culture_vol = [str(row[4]) for row in selection_result]
        media_vol = [str(row[5]) for row in selection_result]

        log(f"Cytomat positions count: {len(cytomat_pos)}")
        log(f"Spillover positions count: {len(spill_positions)}")
        log(f"Spatial/source positions count: {len(well_positions)}")
        log(f"Culture volume count: {len(culture_vol)}")
        log(f"Media volume count: {len(media_vol)}")

        if not (
            len(spill_positions)
            == len(well_positions)
            == len(culture_vol)
            == len(media_vol)
        ):
            log("ERROR: Output list lengths are inconsistent.")
            sys.exit(1)

    except Exception as e:
        log(f"ERROR preparing output lists: {e}")
        sys.exit(1)

    # === Define file paths ===
    cyto_pos_path = f"C:\\EvoTaskFiles\\{run_id}_CytomatPos.txt"
    culture_vol_path = f"C:\\EvoTaskFiles\\{run_id}_CultureVol.txt"
    media_vol_path = f"C:\\EvoTaskFiles\\{run_id}_MediaVol.txt"
    spillover_plate_seq_path = f"C:\\EvoTaskFiles\\{run_id}_SpillOverPlate_Positions.txt"
    spatial_plate_seq_path = f"C:\\EvoTaskFiles\\{run_id}_SpatialOverPlate_Positions.txt"

    # === Write CytomatPos ===
    log("Writing CytomatPos.txt...")
    with open(cyto_pos_path, "w", encoding="utf-8") as f:
        f.write("\n".join(cytomat_pos))

    log(f"File generated: {cyto_pos_path}")

    # === Write Hamilton-formatted sequence files ===
    write_hamilton_format(
        spillover_plate_seq_path,
        spill_positions,
        SpillOverPlate,
        "seqSpillOverPlate",
    )

    write_hamilton_format(
        spatial_plate_seq_path,
        well_positions,
        SpatialEvoPlate,
        "seqEvoSrcPlate",
    )

    # === Write volumes ===
    log("Writing CultureVol.txt...")
    with open(culture_vol_path, "w", encoding="utf-8") as f:
        f.write("\n".join(culture_vol))

    log(f"File generated: {culture_vol_path}")

    log("Writing MediaVol.txt...")
    with open(media_vol_path, "w", encoding="utf-8") as f:
        f.write("\n".join(media_vol))

    log(f"File generated: {media_vol_path}")

    conn.close()
    log("Database connection closed.")
    log("All files generated successfully. === Script completed ===")

    sys.exit(0)

except Exception as e:
    log(f"Fatal error: {e}")
    try:
        conn.close()
    except Exception:
        pass
    sys.exit(1)