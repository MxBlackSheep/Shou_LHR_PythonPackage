"""Plan Champions propagation in Python and export aligned VENUS transfers."""

import argparse

from hamilton_workflows import database, propagation_store as store
from hamilton_workflows.config import Settings
from hamilton_workflows.propagation import calculate_plan, serialize_plan
from hamilton_workflows.propagation_files import stage_files, publish_files
from hamilton_workflows.runtime import RunLogger, run_logged


def propagate(barcode, source_labware, spillover_labware, settings, log, *, run_id=None, dry_run=False):
    with database.connection(settings) as conn:
        cursor = conn.cursor()
        # Do not hold serializable lineage locks while waiting for another
        # planner's application lock: that would block its eventual inserts.
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED; SET XACT_ABORT ON;")
        run_id = run_id or database.latest_run_id(cursor)
        context = store.load_context(cursor, barcode, run_id)
        export = {"source_labware": source_labware, "spillover_labware": spillover_labware,
                  "layout_path": settings.layout_path}
        log(f"Planning source plate {context['source_plate_id']} for run {run_id}.")
        try:
            store.acquire_locks(cursor, context)
            cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            if store.load_context(cursor, barcode, run_id) != context:
                raise RuntimeError("Experiment/source context changed while acquiring propagation locks.")
            plan = calculate_plan(store.load_snapshot(cursor, context), **export)
            if dry_run:
                report = serialize_plan(plan)
                print(report)
                log(f"Dry-run plan (no database or VENUS output writes): {report}")
                return plan
            with stage_files(plan, settings.task_dir) as paths:
                store.persist_plan(cursor, plan)
                conn.commit()
                try:
                    publish_files(paths, settings.task_dir)
                except Exception as error:
                    raise RuntimeError(
                        f"Database committed but output publication failed for run {run_id}, source {barcode}. "
                        "Stop VENUS. Do not rerun propagation: database writes have committed. Manual reconciliation of database records, output files and physical transfers is required."
                    ) from error
            log(f"Published {len(plan['transfers'])} transfers. Physical transfer completion is tracked by VENUS.")
            return plan
        finally:
            # Release the session-owned lock explicitly even with ODBC pooling.
            conn.rollback()
            try:
                cursor.execute("""
                    DECLARE @result int;
                    EXEC @result=sys.sp_releaseapplock @Resource=?, @LockOwner='Session';
                    SELECT @result;
                """, (f"ChampionsExport:{run_id}",))
                cursor.fetchone()
            finally:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
                conn.rollback()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("PlateChainBarcode", help="Source plate barcode")
    parser.add_argument("SpatialEvoPlateID", help="Source VENUS labware identifier")
    parser.add_argument("SpillOverPlateID", help="Destination VENUS labware identifier")
    parser.add_argument("--run-id", help="Explicit Hamilton run ID; defaults to the latest run")
    parser.add_argument("--dry-run", action="store_true", help="Report the plan without database or VENUS output writes")
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    log = RunLogger(settings.log_dir)
    return run_logged(lambda: propagate(
        args.PlateChainBarcode, args.SpatialEvoPlateID, args.SpillOverPlateID, settings, log,
        run_id=args.run_id, dry_run=args.dry_run), log)


if __name__ == "__main__":
    raise SystemExit(main())
