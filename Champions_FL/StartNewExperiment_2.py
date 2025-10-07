import pyodbc
import os
import sys
from datetime import datetime

# === Set up log file ===
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

def main():
    try:
        log("Establishing connection...")
        conn = establish_connection()
        cursor = conn.cursor()
        log("Connected to database.")

        # Get PlateID
        cursor.execute("SELECT TOP 1 PlateID FROM AncestPlatesInExperiments ORDER BY ExperimentID DESC")
        plateID = cursor.fetchone()[0]
        log(f"Retrieved PlateID: {plateID}")

        # Call stored procedure
        cursor.execute("EXEC SpatialEvo_CommenceExperimentFl @PlateID = ?", plateID)
        try:
            result = cursor.fetchone()
        except pyodbc.ProgrammingError:
            result = None
            log("No result returned from stored procedure.")

        if result:
            if isinstance(result[0], str) and "DATABASE ERROR" in result[0]:
                log(f"Stored procedure returned error: {result[0]}")
                sys.exit(1)
            else:
                log(f"Stored procedure returned: {result}")
        else:
            log("Stored procedure completed successfully with no result.")

        conn.commit()
        conn.close()
        log("Connection closed. Script completed successfully.")
        sys.exit(0)

    except Exception as e:
        log(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
