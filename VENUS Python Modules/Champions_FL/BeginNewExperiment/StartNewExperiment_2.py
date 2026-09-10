"""Commence the experiment after its initial fluorescence upload."""

import pyodbc

from hamilton_workflows import database
from hamilton_workflows.config import Settings
from hamilton_workflows.runtime import RunLogger, run_logged


def commence_experiment(settings, log):
    with database.connection(settings) as conn:
        cursor = conn.cursor()
        plate_id = database.latest_ancestor_plate_id(cursor)
        log(f"Retrieved PlateID: {plate_id}")
        cursor.execute("EXEC SpatialEvo_CommenceExperimentFl @PlateID = ?", plate_id)
        try:
            result = cursor.fetchone()
        except pyodbc.ProgrammingError:
            result = None
        if result and isinstance(result[0], str) and "DATABASE ERROR" in result[0]:
            raise RuntimeError(f"Stored procedure returned error: {result[0]}")
        log(f"Stored procedure returned: {result}")
        conn.commit()
    log("Connection closed. Script completed successfully.")


def main(argv=None):
    # VENUS passes two barcodes, which this step has always ignored.
    settings = Settings.from_env()
    log = RunLogger(settings.log_dir)
    return run_logged(lambda: commence_experiment(settings, log), log)

if __name__ == "__main__":
    raise SystemExit(main())
