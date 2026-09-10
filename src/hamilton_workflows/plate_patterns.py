"""Well order and Excel plate assignments shared by experiment setup dialogs."""

import pandas as pd


def wells_96():
    """Hamilton column-major order: A1, B1, ... H1, A2, ... H12."""
    return [f"{row}{column}" for column in range(1, 13) for row in "ABCDEFGH"]


def process_excel_to_well_assignment(excel_file, run_id, plate_id):
    frame = pd.read_excel(excel_file, header=1)
    if not {"Destination", "Source", "Vol"}.issubset(frame.columns):
        raise ValueError("Excel file missing required columns: Destination, Source, Vol")
    frame = frame.dropna(subset=["Destination", "Source"]).copy()
    frame["Destination"] = frame["Destination"].astype(str).str.strip()
    frame["Source"] = frame["Source"].astype(str).str.strip()
    return pd.DataFrame({
        "plateid": plate_id,
        "wellID": frame["Destination"],
        "runid": run_id,
        "WellAssign": frame["Source"].apply(lambda source: "MediaCtrl" if source == "T" else "Cells"),
    })
