"""Gen5 parsing and simulated fluorescence; no database or process side effects."""

from pathlib import Path
import random
import pandas as pd

from .plate_patterns import wells_96
from .runtime import discard_log


def parse_raw_data(raw_path, plate_id, run_id, *, log=discard_log):
    with open(raw_path, "r", encoding="utf-8") as source:
        lines = [line.strip() for line in source if line.strip()]
    log(f"Read {len(lines)} lines from raw data file: {raw_path}")
    if len(lines) < 3:
        raise ValueError("Raw file appears too short or malformed (need headers + data).")
    wells, values = [], []
    for line in lines[2:]:
        parts = line.replace("\t", " ").split()
        if len(parts) < 2:
            continue
        token = parts[-1].strip().rstrip(".")
        if token in ("", "-", "."):
            token = "0"
        try:
            value = float(token)
        except ValueError:
            filtered = "".join(ch for ch in token if ch.isdigit() or ch in ".-")
            try:
                value = float(filtered)
            except ValueError:
                value = 0.0
        wells.append(parts[0].strip())
        values.append(value)
    if not wells:
        raise ValueError("No well data parsed from raw file.")
    frame = pd.DataFrame({"PlateID": plate_id, "WellID": wells, "Value": values, "RunID": run_id})
    log(f"Parsed {len(frame)} wells from raw data file.")
    return frame


def generate_random_raw_file(raw_path, header_pair, *, rng=random, log=discard_log):
    """Preserve the existing test-mode distribution and Gen5 header format."""
    Path(raw_path).parent.mkdir(parents=True, exist_ok=True)
    lines = [header_pair, f"Well\t{header_pair} - Mean"]
    for well in wells_96():
        sample = rng.random()
        if sample < 0.70:
            value = rng.uniform(0, 50)
        elif sample < 0.95:
            value = rng.uniform(50, 500)
        else:
            value = rng.uniform(500, 2200)
        token = f"{int(round(value))}." if rng.random() < 0.4 else str(int(round(value)))
        lines.append(f"{well}\t{token}")
    with open(raw_path, "w", encoding="utf-8") as output:
        output.write("\n".join(lines) + "\n")
    log(f"Generated TEST raw file with random readings: {raw_path}")
    return raw_path


def generate_two_dfs(plate_id, run_id, *, rng=random):
    wells = wells_96()
    return tuple(
        pd.DataFrame({
            "PlateID": plate_id, "WellID": wells,
            channel: [round(rng.uniform(0, 550), 6) for _ in wells], "RunID": run_id,
        })
        for channel in ("FlEx482Em510", "FlEx587Em611")
    )
