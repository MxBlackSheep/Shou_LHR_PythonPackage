import pyodbc
import os
import sys
import pandas as pd
import random
from datetime import datetime

# === Set up logging ===
log_dir = r"C:\Python Log"
os.makedirs(log_dir, exist_ok=True)
script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(log_dir, f"{script_name}_{timestamp}.log")

def log(message):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{now}] {message}\n")

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

def get_runID():
    conn = establish_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT TOP 1 RunGUID FROM HamiltonVectorDB.dbo.HxRun ORDER BY StartTime DESC")
    run_id = cursor.fetchone()[0]
    log(f"Retrieved RunGUID: {run_id}")
    conn.commit()
    conn.close()
    return run_id

def generate_two_dfs(plate_id, run_id):
    rows = ["A", "B", "C", "D", "E", "F", "G", "H"]
    cols = range(1, 13)
    well_ids = [f"{r}{c}" for c in cols for r in rows]

    def random_volume():
        return round(random.uniform(0, 550), 6)

    df1 = pd.DataFrame({
        "plateid": plate_id,
        "wellID": well_ids,
        "volume": [random_volume() for _ in well_ids],
        "runid": run_id
    })

    df2 = pd.DataFrame({
        "plateid": plate_id,
        "wellID": well_ids,
        "volume": [random_volume() for _ in well_ids],
        "runid": run_id
    })

    log(f"Generated fluorescence data for {len(well_ids)} wells.")
    return df1, df2

def filter_fluorescence_to_valid_wells(df, plateID, conn):
    query = "SELECT WellID FROM ImportPlatePattern WHERE PlateID = ?"
    existing_wells = pd.read_sql(query, conn, params=[plateID])["WellID"].unique()
    filtered_df = df[df["wellID"].isin(existing_wells)].copy()
    log(f"Filtered {len(filtered_df)} valid wells out of {len(df)} total.")
    return filtered_df

def main():
    try:
        conn = establish_connection()
        cursor = conn.cursor()
        log("Database connection established.")

        runID = get_runID()

        cursor.execute("SELECT TOP 1 PlateID FROM AncestPlatesInExperiments ORDER BY ExperimentID DESC")
        plateID = cursor.fetchone()[0]
        log(f"Retrieved PlateID: {plateID}")
        conn.commit()
        conn.close()

        EM510, EM611 = generate_two_dfs(plateID, runID)

        conn = establish_connection()
        cursor = conn.cursor()

        EM510 = filter_fluorescence_to_valid_wells(EM510, plateID, conn)
        EM611 = filter_fluorescence_to_valid_wells(EM611, plateID, conn)

        data_510 = list(EM510.itertuples(index=False, name=None))
        data_611 = list(EM611.itertuples(index=False, name=None))

        cursor.executemany("""
            INSERT INTO ImportFlEx482Em510 (PlateID, WellID, FlEx482Em510, RunID)
            VALUES (?, ?, ?, ?)
        """, data_510)
        log(f"Inserted {len(data_510)} rows into ImportFlEx482Em510.")

        cursor.executemany("""
            INSERT INTO ImportFlEx587Em611 (PlateID, WellID, FlEx587Em611, RunID)
            VALUES (?, ?, ?, ?)
        """, data_611)
        log(f"Inserted {len(data_611)} rows into ImportFlEx587Em611.")

        conn.commit()
        conn.close()
        log("Fluorescence data insertion completed successfully.")
        sys.exit(0)

    except Exception as e:
        log(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
