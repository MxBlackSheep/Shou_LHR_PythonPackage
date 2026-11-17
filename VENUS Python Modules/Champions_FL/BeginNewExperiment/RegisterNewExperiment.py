"""Register a Champions experiment and its base layout before plates are known."""

import argparse
import os
import sys
import tkinter as tk
from tkinter import messagebox

from StartNewExperiment_1 import InputForm as ExperimentInputForm
from hamilton_workflows import database
from hamilton_workflows.config import Settings
from hamilton_workflows.plate_patterns import process_excel_to_well_assignment
from hamilton_workflows.propagation import validate_fraction
from hamilton_workflows.runtime import RunLogger, run_logged


def insert_experiment(cursor, user_id, note, schedule, parameters, excel_file):
    """Insert registration records in the caller's transaction; never commit."""
    validate_fraction(parameters.get('TopFractionToPropagate'))
    # The shared parser's run/plate columns are unused for an experiment pattern.
    frame = process_excel_to_well_assignment(excel_file, None, None)
    cursor.execute(
        'INSERT INTO Experiments (UserDefinedID, Note, ScheduledToRun) VALUES (?, ?, ?)',
        (user_id, note, schedule),
    )
    experiment_id = database.fetch_one_value(
        cursor, 'SELECT ExperimentID FROM Experiments WHERE UserDefinedID = ?', (user_id,)
    )
    if experiment_id is None:
        raise RuntimeError(f'Could not retrieve ExperimentID for UserDefinedID={user_id}')
    for name, value in parameters.items():
        cursor.execute(
            'INSERT INTO ExperimentParameters (ExperimentID, ParameterName, ParamValueTxt) VALUES (?, ?, ?)',
            (experiment_id, name, value),
        )
    data = [(experiment_id, well, assignment)
            for well, assignment in frame[['wellID', 'WellAssign']].itertuples(index=False, name=None)]
    if data:
        cursor.executemany(
            'INSERT INTO ExperimentPlatePattern (ExperimentID, WellID, WellAssign) VALUES (?, ?, ?)', data
        )
    if str(schedule) == '1':
        cursor.execute(
            'EXEC dbo.ScheduleExperimentForImmediateExecution @UserDefinedExptID = ?', (user_id,)
        )
    return experiment_id


def create_experiment(user_id, note, schedule, parameters, excel_file, settings, log):
    with database.connection(settings) as conn:
        experiment_id = insert_experiment(
            conn.cursor(), user_id, note, schedule, parameters, excel_file
        )
        conn.commit()
    log(f'Experiment registration complete: {experiment_id}.')
    return experiment_id


class InputForm(ExperimentInputForm):
    def __init__(self, root, settings, log):
        # Reuse the existing widgets, defaults and file picker unchanged. The
        # legacy constructor stores barcodes but does not display or use them.
        super().__init__(root, None, None, settings, log)

    def validate_and_submit(self):
        # Submission is local because the legacy method calls its plate workflow
        # directly. Keep its field validation and success/error behaviour.
        try:
            user_id = self.user_id.get().strip()
            if not user_id:
                raise ValueError('UserDefinedID is required')
            note = self.note.get().strip()
            schedule = self.schedule.get().strip() or '0'
            excel_file = self.excel_path.get()
            if not os.path.exists(excel_file):
                raise FileNotFoundError('Excel file not selected or does not exist')
            parameters = {name: float(entry.get().strip()) for name, entry in self.param_entries.items()}
            create_experiment(user_id, note, schedule, parameters, excel_file, self.settings, self.log)
            messagebox.showinfo('Success', 'Experiment Has Been Created')
            self.root.destroy()
            sys.exit(0)
        except Exception as error:
            self.log(f'Validation or DB error: {error}')
            sys.exit(1)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Register an experiment and Excel base layout without barcodes.')
    parser.parse_args(argv)
    settings = Settings.from_env()
    log = RunLogger(settings.log_dir)

    def launch():
        root = tk.Tk()
        InputForm(root, settings, log)
        root.mainloop()

    return run_logged(launch, log)


if __name__ == '__main__':
    raise SystemExit(main())
