"""Seed initial simulated fluorescence for the assigned wells on the newest ancestor plate."""

import pandas as pd

from hamilton_workflows import database, fluorescence
from hamilton_workflows.config import Settings
from hamilton_workflows.runtime import RunLogger, run_logged


def seed_initial_readings(settings, log):
    """Legacy initial simulation filters to ImportPlatePattern before SQL insert."""
    with database.connection(settings) as conn:
        cursor = conn.cursor()
        run_id = database.latest_run_id(cursor)
        plate_id = database.latest_ancestor_plate_id(cursor)

    frames = fluorescence.generate_two_dfs(plate_id, run_id)
    with database.connection(settings) as conn:
        cursor = conn.cursor()
        for frame, channel in zip(frames, ("FlEx482Em510", "FlEx587Em611")):
            existing_wells = pd.read_sql(
                "SELECT WellID FROM ImportPlatePattern WHERE PlateID = ?", conn, params=[plate_id]
            )["WellID"].unique()
            filtered = frame[frame["WellID"].isin(existing_wells)]
            data = list(filtered.itertuples(index=False, name=None))
            cursor.executemany(
                f"INSERT INTO Import{channel} (PlateID, WellID, {channel}, RunID) VALUES (?, ?, ?, ?)",
                data,
            )
            log(f"Inserted {len(data)} rows into Import{channel}.")
        conn.commit()
    log("Fluorescence data insertion completed successfully.")


def main(argv=None):
    # Like the original no-argument workflow, tolerate extra VENUS arguments.
    settings = Settings.from_env()
    log = RunLogger(settings.log_dir)
    return run_logged(lambda: seed_initial_readings(settings, log), log)

if __name__ == "__main__":
    raise SystemExit(main())
