"""Set up a Champions experiment: collect input, create records and write task files."""

import argparse
import os
import sys
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from hamilton_workflows.config import Settings
from hamilton_workflows.runtime import RunLogger, run_logged
from hamilton_workflows import database
from hamilton_workflows.plate_patterns import process_excel_to_well_assignment
from hamilton_workflows.propagation import PROPAGATION_FRACTIONS, validate_fraction
from hamilton_workflows.task_files import task_path, write_text


def create_experiment(user_id, note, schedule, parameters, barcode1, barcode2, excel_file, settings, log):
    # Validate before the first database write, including non-GUI callers.
    validate_fraction(parameters.get('TopFractionToPropagate'))
    with database.connection(settings) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO Experiments (UserDefinedID, Note, ScheduledToRun) VALUES (?, ?, ?)",
            (user_id, note, schedule),
        )
        experiment_id = database.fetch_one_value(
            cursor, "SELECT ExperimentID FROM Experiments WHERE UserDefinedID = ?", (user_id,)
        )
        if experiment_id is None:
            raise RuntimeError(f"Could not retrieve ExperimentID for UserDefinedID={user_id}")
        for name, value in parameters.items():
            cursor.execute(
                "INSERT INTO ExperimentParameters (ExperimentID, ParameterName, ParamValueTxt) VALUES (?, ?, ?)",
                (experiment_id, name, value),
            )
        cursor.execute(
            "EXEC SpatialEvo_NewExperiment @UserExpID = ?, @NewPlateBC = ?, @ExpandPlateBC = ?",
            user_id, barcode1, barcode2,
        )
        result = cursor.fetchone()
        if result and "DATABASE ERROR" in str(result[0]):
            raise RuntimeError(f"Stored proc error: {result[0]}")
        if result is None:
            raise RuntimeError("SpatialEvo_NewExperiment returned no plate information.")
        plate_id, cytomat_pos = result
        expansion_pos = database.fetch_one_value(cursor, "SELECT dbo.QueryCytomatPosition(?)", (barcode2,))
        run_id = database.latest_run_id(cursor)
        log(f"Retrieved RunGUID: {run_id}; PlateID: {plate_id}; Cytomat Position: {cytomat_pos}")

        for suffix, value in (("PlateID", plate_id), ("CytomatPos", cytomat_pos), ("ExpansionCytomatPos", expansion_pos)):
            if value is not None:
                path = task_path(settings.task_dir, run_id, suffix)
                write_text(path, value)
                log(f"Written {suffix} to {path}")

        frame = process_excel_to_well_assignment(excel_file, run_id, plate_id)
        data = list(frame.itertuples(index=False, name=None))
        cursor.executemany(
            "INSERT INTO ImportPlatePattern (PlateID, WellID, RunID, WellAssign) VALUES (?, ?, ?, ?)", data
        )
        frame.to_csv("output.txt", sep="\t", index=False)
        log(f"Inserted {len(data)} rows into ImportPlatePattern and wrote output.txt.")
        conn.commit()
    log("Experiment creation complete.")
    return plate_id, cytomat_pos


class InputForm:
    def __init__(self, root, barcode1, barcode2, settings, log):
        self.settings = settings
        self.log = log
        self.root = root
        self.barcode1 = barcode1
        self.barcode2 = barcode2
        self.root.title("Starting A New Experiment")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self.excel_path = tk.StringVar()

        self.parameter_defaults = {
            'BackgroundOD': 0.036,
            'InoculationOD': 0.03,
            'MaxIteration': 20,
            'ODConversionFactor': 2.52,
            'FreezeVolume': 0.5,
            'TargetWellVolume': 700,
            'V_OD_Sample': 150,
            'TopFractionToPropagate': 1,
            'FreezeGeneration': 1,
            'UseFluorescence': 1,
            'GFP_scale': 4669.1,
            'GFP_RFPdamping': 773.1,
            'RFP_scale': 2262.3,
            'RFP_GFPdamping': 305.0
        }

        self.frame = ttk.Frame(root, padding="10")
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Basic inputs
        ttk.Label(self.frame, text="UserDefinedID:").grid(row=0, column=0, sticky=tk.W)
        self.user_id = ttk.Entry(self.frame)
        self.user_id.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(self.frame, text="Note:").grid(row=1, column=0, sticky=tk.W)
        self.note = ttk.Entry(self.frame)
        self.note.grid(row=1, column=1, padx=5, pady=5)

        self.schedule_var = tk.StringVar(value='0')
        ttk.Label(self.frame, text="ScheduleToRun[0/1]:").grid(row=2, column=0, sticky=tk.W)
        self.schedule = ttk.Combobox(self.frame, textvariable=self.schedule_var, values=["0", "1"], state="readonly")
        self.schedule.grid(row=2, column=1, padx=5, pady=5)

        # Excel file selection
        ttk.Label(self.frame, text="Excel File:").grid(row=3, column=0, sticky=tk.W)
        self.excel_entry = ttk.Entry(self.frame, textvariable=self.excel_path, width=40)
        self.excel_entry.grid(row=3, column=1, padx=5, pady=5)
        ttk.Button(self.frame, text="Browse...", command=self.browse_excel).grid(row=3, column=2, padx=5)

        param_frame = ttk.LabelFrame(self.frame, text="Parameters", padding="5")
        param_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10)

        self.param_entries = {}
        for i, (param, default) in enumerate(self.parameter_defaults.items()):
            row = i // 2
            col = i % 2 * 2
            ttk.Label(param_frame, text=f"{param}:").grid(row=row, column=col, sticky=tk.W, padx=5)
            if param == 'TopFractionToPropagate':
                entry = ttk.Combobox(param_frame, values=[f"{v:g}" for v in PROPAGATION_FRACTIONS], state="readonly", width=18)
                entry.set(str(default))
            else:
                entry = ttk.Entry(param_frame, width=20)
                entry.insert(0, str(default))
            entry.grid(row=row, column=col + 1, padx=5, pady=2)
            self.param_entries[param] = entry

        ttk.Button(self.frame, text="Submit", command=self.validate_and_submit).grid(row=5, column=0, columnspan=3, pady=10)

    def on_close(self):
        self.log("User closed the GUI window without submitting.")
        sys.exit(1)

    def browse_excel(self):
        filename = filedialog.askopenfilename(title="Select Excel File", filetypes=[("Excel files", "*.xlsx *.xls")])
        if filename:
            self.excel_path.set(filename)
            self.log(f"Excel file selected: {filename}")

    def validate_and_submit(self):
        try:
            user_id = self.user_id.get().strip()
            if not user_id:
                raise ValueError("UserDefinedID is required")
            note = self.note.get().strip()
            schedule = self.schedule.get().strip() or "0"
            excel_file = self.excel_path.get()
            if not os.path.exists(excel_file):
                raise FileNotFoundError("Excel file not selected or does not exist")
            parameters = {name: float(entry.get().strip()) for name, entry in self.param_entries.items()}
            create_experiment(
                user_id, note, schedule, parameters, self.barcode1, self.barcode2,
                excel_file, self.settings, self.log,
            )
            messagebox.showinfo("Success", "Experiment Has Been Created")
            self.root.destroy()
            sys.exit(0)
        except Exception as error:
            self.log(f"Validation or DB error: {error}")
            sys.exit(1)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Launch experiment setup GUI.")
    parser.add_argument("barcode1", type=str, help="New Plate Barcode identifier")
    parser.add_argument("barcode2", type=str, help="Expansion Plate Barcode identifier")
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    log = RunLogger(settings.log_dir)

    def launch():
        root = tk.Tk()
        InputForm(root, args.barcode1, args.barcode2, settings, log)
        root.mainloop()

    return run_logged(launch, log)

if __name__ == "__main__":
    raise SystemExit(main())
