import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
import pyodbc
import os
import pandas as pd
import argparse
import sys


# === Setup logging ===
log_dir = r"C:\Python Log"
os.makedirs(log_dir, exist_ok=True)

script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(log_dir, f"{script_name}_{timestamp}.log")


def log(message):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{now}] {message}\n")


# === Parse CLI Arguments ===
def parse_args():
    parser = argparse.ArgumentParser(description="Launch experiment setup GUI.")
    parser.add_argument("barcode", type=str, help="New Plate Barcode identifier")
    return parser.parse_args()


class InputForm:
    def __init__(self, root, barcode):
        self.root = root
        self.barcode = barcode
        self.root.title("Starting A New Experiment")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self.excel_path = tk.StringVar()

        # === Updated parameter set ===
        # These are inserted into ExperimentParameters.
        self.parameter_defaults = {
            "BackgroundOD": 0.036,
            "InoculationOD": 0.03,
            "ODConversionFactor": 2.52,
            "TargetWellVolume": 700,
            "TopFractionToPropagate": 1,
            "is_test": 0,
        }

        self.frame = ttk.Frame(root, padding="10")
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # === Basic inputs ===
        ttk.Label(self.frame, text="UserDefinedID:").grid(row=0, column=0, sticky=tk.W)
        self.user_id = ttk.Entry(self.frame)
        self.user_id.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(self.frame, text="Note:").grid(row=1, column=0, sticky=tk.W)
        self.note = ttk.Entry(self.frame)
        self.note.grid(row=1, column=1, padx=5, pady=5)

        self.schedule_var = tk.StringVar(value="0")
        ttk.Label(self.frame, text="ScheduleToRun[0/1]:").grid(row=2, column=0, sticky=tk.W)
        self.schedule = ttk.Combobox(
            self.frame,
            textvariable=self.schedule_var,
            values=["0", "1"],
            state="readonly",
        )
        self.schedule.grid(row=2, column=1, padx=5, pady=5)

        # === Excel file selection ===
        ttk.Label(self.frame, text="Excel File:").grid(row=3, column=0, sticky=tk.W)
        self.excel_entry = ttk.Entry(self.frame, textvariable=self.excel_path, width=40)
        self.excel_entry.grid(row=3, column=1, padx=5, pady=5)

        ttk.Button(
            self.frame,
            text="Browse...",
            command=self.browse_excel,
        ).grid(row=3, column=2, padx=5)

        # === Parameters ===
        param_frame = ttk.LabelFrame(self.frame, text="Parameters", padding="5")
        param_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10)

        self.param_entries = {}

        for i, (param, default) in enumerate(self.parameter_defaults.items()):
            row = i // 2
            col = (i % 2) * 2

            ttk.Label(param_frame, text=f"{param}:").grid(
                row=row,
                column=col,
                sticky=tk.W,
                padx=5,
            )

            if param == "TopFractionToPropagate":
                entry = ttk.Combobox(
                    param_frame,
                    values=["0.25", "0.5", "0.75", "1"],
                    state="readonly",
                    width=18,
                )
                entry.set(str(default))

            elif param == "is_test":
                entry = ttk.Combobox(
                    param_frame,
                    values=["0", "1"],
                    state="readonly",
                    width=18,
                )
                entry.set(str(default))

            else:
                entry = ttk.Entry(param_frame, width=20)
                entry.insert(0, str(default))

            entry.grid(row=row, column=col + 1, padx=5, pady=2)
            self.param_entries[param] = entry

        ttk.Button(
            self.frame,
            text="Submit",
            command=self.validate_and_submit,
        ).grid(row=5, column=0, columnspan=3, pady=10)

    def on_close(self):
        log("User closed the GUI window without submitting.")
        sys.exit(1)

    def browse_excel(self):
        filename = filedialog.askopenfilename(
            title="Select Excel File",
            filetypes=[("Excel files", "*.xlsx *.xls")],
        )

        if filename:
            self.excel_path.set(filename)
            log(f"Excel file selected: {filename}")

    def establish_connection(self):
        log("Establishing database connection...")

        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 11 for SQL Server};"
            "SERVER=LOCALHOST\\HAMILTON;"
            "DATABASE=EvoYeast;"
            "UID=Hamilton;"
            "PWD=mkdpw:V43;"
            "Trust_Connection=no;"
        )

        log("Database connection established.")
        return conn

    def get_runID(self):
        conn = self.establish_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT TOP 1 RunGUID
            FROM HamiltonVectorDB.dbo.HxRun
            ORDER BY StartTime DESC
            """
        )

        row = cursor.fetchone()
        conn.close()

        if row is None:
            raise RuntimeError("Could not retrieve RunGUID from HamiltonVectorDB.dbo.HxRun")

        run_id = row[0]
        log(f"Retrieved RunGUID: {run_id}")

        return run_id

    def process_excel_to_well_assignment(self, excel_file, runid, plateid):
        df = pd.read_excel(excel_file, header=1).dropna(subset=["Destination", "Source"])

        required_columns = {"Destination", "Source", "Vol"}
        if not required_columns.issubset(df.columns):
            raise ValueError(f"Excel file missing required columns: {required_columns}")

        df["Destination"] = df["Destination"].astype(str).str.strip()
        df["Source"] = df["Source"].astype(str).str.strip()

        log(f"Processing {len(df)} rows from Excel.")

        return pd.DataFrame({
            "plateid": plateid,
            "wellID": df["Destination"],
            "runid": runid,
            "WellAssign": df["Source"].apply(lambda x: "MediaCtrl" if x == "T" else "Cells"),
        })

    def insert_experiment_record(self, cursor, user_id, note, schedule, parameters):
        """
        Inserts the new experiment into Experiments.

        Inserts all parameters, including is_test, into ExperimentParameters.
        """

        cursor.execute(
            """
            INSERT INTO Experiments (UserDefinedID, Note, ScheduledToRun)
            VALUES (?, ?, ?)
            """,
            (user_id, note, schedule),
        )

        log(f"Inserted Experiment: UserDefinedID={user_id}, ScheduledToRun={schedule}")

        cursor.execute(
            """
            SELECT ExperimentID
            FROM Experiments
            WHERE UserDefinedID = ?
            """,
            (user_id,),
        )

        row = cursor.fetchone()
        if row is None:
            raise RuntimeError(f"Could not retrieve ExperimentID for UserDefinedID={user_id}")

        experiment_id = row[0]
        log(f"Retrieved ExperimentID: {experiment_id}")

        for param_name, param_value in parameters.items():
            cursor.execute(
                """
                INSERT INTO ExperimentParameters (ExperimentID, ParameterName, ParamValueTxt)
                VALUES (?, ?, ?)
                """,
                (experiment_id, param_name, str(param_value)),
            )

        log(f"Inserted {len(parameters)} parameters into ExperimentParameters.")

        return experiment_id

    def create_new_experiment_records(self, cursor, user_id, new_plate_barcode):
        """
        Python-integrated replacement for the original stored procedure:

            dbo.SpatialEvo_NewExperiment

        Main changes:
        - one plate only
        - no expansion plate
        - no propagation anchor between new plate and expansion plate
        """

        log("Creating new experiment records without SpatialEvo_NewExperiment.")
        log(f"UserDefinedID={user_id}, NewPlateBarcode={new_plate_barcode}")

        # === Barcode conflict check ===
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM Plates
            WHERE BarCode = ?
            """,
            (new_plate_barcode,),
        )

        barcode_count = cursor.fetchone()[0]

        if barcode_count != 0:
            raise RuntimeError(f"BARCODE CONFLICT: barcode already exists in Plates: {new_plate_barcode}")

        # === Experiment existence check ===
        cursor.execute(
            """
            SELECT ExperimentID
            FROM Experiments
            WHERE UserDefinedID = ?
            """,
            (user_id,),
        )

        row = cursor.fetchone()

        if row is None:
            raise RuntimeError(f"Can not find experiment with UserDefinedID={user_id}")

        experiment_id = row[0]
        log(f"Confirmed existing ExperimentID: {experiment_id}")

        # === Insert new plate ===
        cursor.execute(
            """
            INSERT INTO Plates (Description, BarCode, CurrentIteration, Discarded)
            VALUES (?, ?, ?, ?)
            """,
            ("", new_plate_barcode, 1, 0),
        )

        cursor.execute(
            """
            SELECT PlateID
            FROM Plates
            WHERE BarCode = ?
            """,
            (new_plate_barcode,),
        )

        row = cursor.fetchone()

        if row is None:
            raise RuntimeError(f"Could not retrieve PlateID for barcode={new_plate_barcode}")

        new_plate_id = row[0]
        log(f"Inserted new PlateID: {new_plate_id}")

        # === Link ancestral plate to experiment ===
        cursor.execute(
            """
            INSERT INTO AncestPlatesInExperiments
            VALUES (?, ?)
            """,
            (experiment_id, new_plate_id),
        )

        log("Inserted AncestPlatesInExperiments record.")

        # === Make experiment active / scheduled through existing database helper ===
        cursor.execute(
            """
            EXECUTE dbo.ScheduleExperimentForImmediateExecution ?
            """,
            (user_id,),
        )

        log("Executed dbo.ScheduleExperimentForImmediateExecution.")

        # === Place new plate into Cytomat through existing database helper ===
        cursor.execute(
            """
            EXECUTE dbo.InsertPlateIntoCytomat ?
            """,
            (new_plate_id,),
        )

        log("Executed dbo.InsertPlateIntoCytomat.")

        # === Set up anchor culture for the new plate ===
        cursor.execute(
            """
            INSERT INTO Cultures
            VALUES (dbo.GenerateCultureID(?, NULL), ?, NULL)
            """,
            (new_plate_id, new_plate_id),
        )

        log("Inserted anchor culture for new plate.")

        # === Retrieve Cytomat position ===
        cursor.execute(
            """
            SELECT PlateID, CytomatPos
            FROM Cytomat
            WHERE PlateID = ?
            """,
            (new_plate_id,),
        )

        row = cursor.fetchone()

        if row is None:
            raise RuntimeError(f"Could not retrieve Cytomat position for PlateID={new_plate_id}")

        plate_id, cytomat_pos = row

        log(f"Retrieved PlateID={plate_id}, CytomatPos={cytomat_pos}")

        return plate_id, cytomat_pos

    def write_task_files(self, run_id, plate_id, cytomat_pos):
        """
        Writes output files used by VENUS / EvoTaskFiles.
        """

        os.makedirs(r"C:\EvoTaskFiles", exist_ok=True)

        plate_id_path = f"C:\\EvoTaskFiles\\{run_id}_PlateID.txt"
        with open(plate_id_path, "w") as f:
            f.write(str(plate_id))

        log(f"Written PlateID to {plate_id_path}")

        cytomat_pos_path = f"C:\\EvoTaskFiles\\{run_id}_CytomatPos.txt"
        with open(cytomat_pos_path, "w") as f:
            f.write(str(cytomat_pos))

        log(f"Written Cytomat Position to {cytomat_pos_path}")

    def validate_and_submit(self):
        conn = None

        try:
            user_id = self.user_id.get().strip()
            if not user_id:
                raise ValueError("UserDefinedID is required")

            note = self.note.get().strip()
            schedule = self.schedule.get().strip() or "0"

            excel_file = self.excel_path.get()
            if not excel_file or not os.path.exists(excel_file):
                raise FileNotFoundError("Excel file not selected or does not exist")

            parameters = {}

            for param, entry in self.param_entries.items():
                raw_value = entry.get().strip()

                if raw_value == "":
                    raise ValueError(f"{param} is required")

                parameters[param] = float(raw_value)

            conn = self.establish_connection()
            cursor = conn.cursor()

            # Keep all database changes in one Python-controlled transaction.
            self.insert_experiment_record(
                cursor=cursor,
                user_id=user_id,
                note=note,
                schedule=schedule,
                parameters=parameters,
            )

            plate_id, cytomat_pos = self.create_new_experiment_records(
                cursor=cursor,
                user_id=user_id,
                new_plate_barcode=self.barcode,
            )

            run_id = self.get_runID()

            self.write_task_files(
                run_id=run_id,
                plate_id=plate_id,
                cytomat_pos=cytomat_pos,
            )

            processed_df = self.process_excel_to_well_assignment(
                excel_file=excel_file,
                runid=run_id,
                plateid=plate_id,
            )

            data = list(processed_df.itertuples(index=False, name=None))

            cursor.executemany(
                """
                INSERT INTO ImportPlatePattern (PlateID, WellID, RunID, WellAssign)
                VALUES (?, ?, ?, ?)
                """,
                data,
            )

            log(f"Inserted {len(data)} rows into ImportPlatePattern.")

            processed_df.to_csv("output.txt", sep="\t", index=False)
            log(f"Wrote output.txt with {len(processed_df)} rows.")

            conn.commit()
            conn.close()

            messagebox.showinfo("Success", "Experiment Has Been Created")
            log("Experiment creation complete.")

            self.root.destroy()
            sys.exit(0)

        except Exception as e:
            log(f"Validation or DB error: {e}")

            if conn is not None:
                try:
                    conn.rollback()
                    conn.close()
                    log("Database transaction rolled back.")
                except Exception as rollback_error:
                    log(f"Rollback/close error: {rollback_error}")

            messagebox.showerror("Error", str(e))
            sys.exit(1)


# === Main Application ===
if __name__ == "__main__":
    try:
        args = parse_args()
        barcode = args.barcode

        root = tk.Tk()
        app = InputForm(root, barcode)
        root.mainloop()

    except Exception as e:
        log(f"Unhandled exception: {e}")
        sys.exit(1)