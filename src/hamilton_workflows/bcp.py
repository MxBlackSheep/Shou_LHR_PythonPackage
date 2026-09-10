"""BCP imports using the robot's existing Windows integrated authentication."""

import os
import subprocess

from .runtime import discard_log


def write_bcp_file(frame, path, *, log=discard_log):
    frame.to_csv(path, sep="\t", header=False, index=False)
    log(f"BCP input file written: {path}")


def run_bcp(table_name, path, settings, *, log=discard_log):
    command = [
        "bcp", f"{settings.database}.dbo.{table_name}", "in", os.fspath(path),
        "-T", "-c", "-S", settings.bcp_server,
    ]
    log(f"Executing BCP silently: {' '.join(command)}")
    result = subprocess.run(
        command, capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW,
    )
    if result.returncode:
        log(f"BCP stdout: {result.stdout}")
        raise RuntimeError(f"BCP import failed ({result.returncode}): {result.stderr}")
    log(f"BCP upload to {table_name} successful. Output: {result.stdout}")
