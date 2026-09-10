"""Select cultures, run Champions propagation, and export aligned VENUS files."""

import argparse

from hamilton_workflows import bcp, database
from hamilton_workflows.config import Settings
from hamilton_workflows.runtime import RunLogger, run_logged
from hamilton_workflows.task_files import task_path, write_hamilton_sequence, write_plain_lines, write_tsv_rows


def is_dummy_propagation_row(row):
    return len(row) >= 7 and tuple(row[:7]) == (0, None, None, None, 0, 0, 0)


def write_propagation_files(rows, run_id, source_labware, spillover_labware, settings):
    if not rows:
        raise RuntimeError("No usable propagation data returned from Champions_CommencePropagationFl.")
    cytomat = [str(row[0]) for row in rows if row[0] is not None]
    spill_positions = [str(row[2]) for row in rows]
    source_positions = [str(row[3]) for row in rows]
    culture_volumes = [str(row[4]) for row in rows]
    media_volumes = [str(row[5]) for row in rows]

    write_plain_lines(task_path(settings.task_dir, run_id, "CytomatPos"), cytomat)
    write_hamilton_sequence(
        task_path(settings.task_dir, run_id, "SpillOverPlate_Positions"),
        spill_positions, spillover_labware, "seqSpillOverPlate", settings.layout_path,
    )
    write_hamilton_sequence(
        task_path(settings.task_dir, run_id, "SpatialOverPlate_Positions"),
        source_positions, source_labware, "seqEvoSrcPlate", settings.layout_path,
    )
    write_plain_lines(task_path(settings.task_dir, run_id, "CultureVol"), culture_volumes)
    write_plain_lines(task_path(settings.task_dir, run_id, "MediaVol"), media_volumes)


def read_experiment_parameters(cursor, names):
    parameters = {}
    for name in names:
        value = database.fetch_one_value(cursor, "SELECT dbo.ReadExperimentParameter(NULL, ?)", (name,))
        if value is None:
            raise RuntimeError(f"Parameter {name} missing.")
        parameters[name] = float(value)
    return parameters


def propagate(barcode, source_labware, spillover_labware, settings, log):
    with database.connection(settings) as conn:
        cursor = conn.cursor()
        run_id = database.latest_run_id(cursor)
        log(f"Latest RunGUID retrieved: {run_id}")
        cursor.execute(
            "EXEC dbo.Competition_SelectCultures @Barcode = ?, @RunID = ?", (barcode, run_id)
        )
        cultures = cursor.fetchall()
        if not cultures:
            raise RuntimeError("No data returned from Competition_SelectCultures.")
        subset_path = task_path(settings.task_dir, run_id, "subset")
        write_tsv_rows(subset_path, [[row[0], run_id, row[1], 1] for row in cultures])
        bcp.run_bcp("ImportSpatialEvoODSubset", subset_path, settings, log=log)

        params = read_experiment_parameters(
            cursor, ("TargetWellVolume", "InoculationOD", "TopFractionToPropagate", "V_OD_Sample")
        )
        for name, value in params.items():
            log(f"Parameter loaded: {name} = {value}")
        cursor.execute(
            "EXEC dbo.Champions_CommencePropagationFl "
            "@TargetVol=?, @RunId=?, @InoculationOD=?, @TopFractionToPropagate=?, @ODSampleVol=?",
            (params["TargetWellVolume"], run_id, params["InoculationOD"],
             params["TopFractionToPropagate"], params["V_OD_Sample"]),
        )
        rows = database.fetch_all_result_sets(cursor, skip_row=is_dummy_propagation_row, log=log)
        # Preserve the stored-procedure commit before exporting the files.
        conn.commit()
        write_propagation_files(rows, run_id, source_labware, spillover_labware, settings)
    log("All files generated successfully. === Script completed ===")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("PlateChainBarcode", type=str, help="PlateChainBarcode identifier")
    parser.add_argument("SpatialEvoPlateID", type=str, help="SpatialEvoPlateID identifier")
    parser.add_argument("SpillOverPlateID", type=str, help="SpillOverPlateID identifier")
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    log = RunLogger(settings.log_dir)
    return run_logged(
        lambda: propagate(args.PlateChainBarcode, args.SpatialEvoPlateID, args.SpillOverPlateID, settings, log),
        log,
    )

if __name__ == "__main__":
    raise SystemExit(main())
