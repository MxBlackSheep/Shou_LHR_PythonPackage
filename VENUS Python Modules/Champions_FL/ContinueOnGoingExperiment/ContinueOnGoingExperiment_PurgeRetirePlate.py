"""Request an expansion plate and write the AddPlate response for VENUS."""

import random

from hamilton_workflows import database
from hamilton_workflows.config import Settings
from hamilton_workflows.runtime import RunLogger, run_logged
from hamilton_workflows.task_files import task_path, write_plain_lines, write_text


def generate_unique_barcode(existing, *, rng=random):
    # Preserve the existing BC1000..BC9999 simulation barcode convention.
    if all(f"BC{number}" in existing for number in range(1000, 10000)):
        raise RuntimeError("No unused expansion barcode remains in BC1000..BC9999.")
    while True:
        barcode = f"BC{rng.randint(1000, 9999)}"
        if barcode not in existing:
            return barcode


def add_expansion_plate(settings, log):
    with database.connection(settings) as conn:
        cursor = conn.cursor()
        run_id = database.latest_run_id(cursor)
        cursor.execute("""
            SELECT AncestPlatesInExperiments.PlateID
            FROM AncestPlatesInExperiments
            INNER JOIN Experiments ON AncestPlatesInExperiments.ExperimentID = Experiments.ExperimentID
            WHERE Experiments.ScheduledToRun = 1
        """)
        row = cursor.fetchone()
        if not row:
            raise RuntimeError("No ancestor plate found.")
        ancestor = row[0]
        cursor.execute("""
            SELECT DISTINCT Plates.PlateID
            FROM Plates
            INNER JOIN dbo.Descendants(?) AS D ON Plates.PlateID = D.DescPlateID
        """, ancestor)
        chain = [ancestor] + [row[0] for row in cursor.fetchall()]
        placeholders = ",".join("?" * len(chain))
        cursor.execute(f"SELECT BarCode FROM Plates WHERE PlateID IN ({placeholders})", chain)
        existing = {str(row[0]) for row in cursor.fetchall() if row[0]}
        barcode = generate_unique_barcode(existing)
        cursor.execute("EXEC dbo.AddExpansionPlateToActiveExperiment @Barcode = ?", (barcode,))
        result = cursor.fetchall()
        conn.commit()
        path = task_path(settings.task_dir, run_id, "AddPlate")
        if result:
            lines = [",".join(str(value) if value is not None else "" for value in row) for row in result]
            write_plain_lines(path, lines, trailing_newline=True)
        else:
            write_text(path, barcode)
        log(f"Result file written: {path}")
    log("=== Script completed successfully ===")


def main(argv=None):
    settings = Settings.from_env()
    log = RunLogger(settings.log_dir)
    return run_logged(lambda: add_expansion_plate(settings, log), log)

if __name__ == "__main__":
    raise SystemExit(main())
