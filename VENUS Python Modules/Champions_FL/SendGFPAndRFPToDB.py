"""Upload both Champions fluorescence channels, optionally generating Gen5 test inputs."""

import argparse

from hamilton_workflows import bcp, database, fluorescence
from hamilton_workflows.config import Settings
from hamilton_workflows.runtime import RunLogger, run_logged
from hamilton_workflows.task_files import add_test_suffix_preserve_ext, task_path


def upload_readings(barcode, settings, log, *, test_mode=False):
    with database.connection(settings) as conn:
        cursor = conn.cursor()
        run_id = database.latest_run_id(cursor)
        plate_id = database.plate_id_from_barcode(cursor, barcode)
    log(f"Retrieved RunGUID: {run_id}; PlateID: {plate_id}")

    channels = ("FlEx482Em510", "FlEx587Em611")
    raw_paths = [settings.gen5_dir / f"{channel}_data" for channel in channels]
    if test_mode:
        log("Running Under test-mode")
        raw_paths = [add_test_suffix_preserve_ext(path) for path in raw_paths]
        for path, header in zip(raw_paths, ("482,510", "587,611")):
            fluorescence.generate_random_raw_file(path, header, log=log)
    frames = [fluorescence.parse_raw_data(path, plate_id, run_id, log=log) for path in raw_paths]

    # Prepare both files before starting the two independent BCP imports.
    paths = [task_path(settings.task_dir, run_id, channel) for channel in channels]
    for frame, path in zip(frames, paths):
        bcp.write_bcp_file(frame, path, log=log)
    for channel, path in zip(channels, paths):
        bcp.run_bcp(f"Import{channel}", path, settings, log=log)
    log("=== Fluorescence data upload completed successfully ===")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("PlateBarcode", type=str, help="Plate Barcode Identifier")
    parser.add_argument(
        "--test-mode", action="store_true",
        help="Generate random *_test raw files and upload them using BCP.",
    )
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    log = RunLogger(settings.log_dir)
    return run_logged(
        lambda: upload_readings(args.PlateBarcode, settings, log, test_mode=args.test_mode), log
    )

if __name__ == "__main__":
    raise SystemExit(main())
