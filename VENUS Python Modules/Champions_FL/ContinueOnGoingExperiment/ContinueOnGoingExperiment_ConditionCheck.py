"""Find plates in the active chain that have selectable cultures."""

from hamilton_workflows import database
from hamilton_workflows.config import Settings
from hamilton_workflows.runtime import RunLogger, run_logged
from hamilton_workflows.task_files import task_path, write_plain_lines


def check_conditions(settings, log):
    with database.connection(settings) as conn:
        cursor = conn.cursor()
        run_id = database.latest_run_id(cursor)
        log(f"Latest RunGUID retrieved: {run_id}")
        cursor.execute("EXEC dbo.Evo_RetrievePlateChain")
        # This procedure's two result sets represent ancestor and descendants.
        rows = list(cursor.fetchall())
        if cursor.nextset():
            rows.extend(cursor.fetchall())
        chains = [(str(row[0]), str(row[1])) for row in rows if row and row[0]]
        if not chains:
            raise RuntimeError("No plates found in chain.")

        valid = []
        for barcode, position in chains:
            cursor.execute(
                "EXEC dbo.Competition_SelectCultures @Barcode = ?, @RunID = ?", [barcode, run_id]
            )
            if cursor.fetchall():
                valid.append(f"{barcode},{position}")
            else:
                log(f"Plate {barcode} has no culture data, skipping.")
        if not valid:
            raise RuntimeError("No valid plates found, file not created.")
        path = task_path(settings.task_dir, run_id, "PlateChainChecked")
        write_plain_lines(path, valid)
        log(f"PlateChainChecked file written: {path}")
    log("=== Script completed successfully ===")


def main(argv=None):
    settings = Settings.from_env()
    log = RunLogger(settings.log_dir)
    return run_logged(lambda: check_conditions(settings, log), log)

if __name__ == "__main__":
    raise SystemExit(main())
