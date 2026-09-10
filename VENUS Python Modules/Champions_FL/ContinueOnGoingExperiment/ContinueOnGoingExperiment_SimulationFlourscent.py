"""Generate and upload uniform simulated fluorescence for the requested plate."""

import argparse

from hamilton_workflows import bcp, database, fluorescence
from hamilton_workflows.config import Settings
from hamilton_workflows.runtime import RunLogger, run_logged
from hamilton_workflows.task_files import task_path


def upload_simulated_readings(barcode, settings, log):
    with database.connection(settings) as conn:
        cursor = conn.cursor()
        run_id = database.latest_run_id(cursor)
        plate_id = database.plate_id_from_barcode(cursor, barcode)
    log(f"Retrieved RunGUID: {run_id}; PlateID: {plate_id}")

    frames = fluorescence.generate_two_dfs(plate_id, run_id)
    channels = ("FlEx482Em510", "FlEx587Em611")
    paths = [task_path(settings.task_dir, run_id, channel) for channel in channels]
    for frame, path in zip(frames, paths):
        bcp.write_bcp_file(frame, path, log=log)
    for channel, path in zip(channels, paths):
        bcp.run_bcp(f"Import{channel}", path, settings, log=log)
    log("=== Fluorescence data upload completed successfully ===")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("PlateBarcode", type=str, help="Plate Barcode Identifier")
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    log = RunLogger(settings.log_dir)
    return run_logged(lambda: upload_simulated_readings(args.PlateBarcode, settings, log), log)

if __name__ == "__main__":
    raise SystemExit(main())
