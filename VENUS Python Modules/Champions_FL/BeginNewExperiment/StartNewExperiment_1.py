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
    parser.add_argument("barcode1", type=str, help="New Plate Barcode identifier")
    parser.add_argument("barcode2", type=str, help="Expansion Plate Barcode identifier")
    return parser.parse_args()

class InputForm:
    def __init__(self, root, barcode1, barcode2):
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
                entry = ttk.Combobox(param_frame, values=["0.25", "0.5", "0.75", "1"], state="readonly", width=18)
                entry.set(str(default))
            else:
                entry = ttk.Entry(param_frame, width=20)
                entry.insert(0, str(default))
            entry.grid(row=row, column=col + 1, padx=5, pady=2)
            self.param_entries[param] = entry

        ttk.Button(self.frame, text="Submit", command=self.validate_and_submit).grid(row=5, column=0, columnspan=3, pady=10)

    def on_close(self):
        log("User closed the GUI window without submitting.")
        sys.exit(1)

    def browse_excel(self):
        filename = filedialog.askopenfilename(title="Select Excel File", filetypes=[("Excel files", "*.xlsx *.xls")])
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
        cursor.execute("SELECT TOP 1 RunGUID FROM HamiltonVectorDB.dbo.HxRun ORDER BY StartTime DESC")
        run_id = cursor.fetchone()[0]
        log(f"Retrieved RunGUID: {run_id}")
        conn.commit()
        conn.close()
        return run_id

    def process_excel_to_well_assignment(self, excel_file, runid, plateid):
        df = pd.read_excel(excel_file, header=1).dropna(subset=["Destination", "Source"])
        if not {"Destination", "Source", "Vol"}.issubset(df.columns):
            raise ValueError("Excel file missing required columns")

        df["Destination"] = df["Destination"].astype(str).str.strip()
        df["Source"] = df["Source"].astype(str).str.strip()

        log(f"Processing {len(df)} rows from Excel.")
        return pd.DataFrame({
            "plateid": plateid,
            "wellID": df["Destination"],
            "runid": runid,
            "WellAssign": df["Source"].apply(lambda x: "MediaCtrl" if x == "T" else "Cells")
        })

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

            parameters = {}
            for param, entry in self.param_entries.items():
                value = float(entry.get().strip())
                parameters[param] = value

            conn = self.establish_connection()
            cursor = conn.cursor()

            cursor.execute("INSERT INTO Experiments (UserDefinedID, Note, ScheduledToRun) VALUES (?, ?, ?)",
                           (user_id, note, schedule))
            log(f"Inserted Experiment: {user_id}, Schedule={schedule}")

            cursor.execute("SELECT ExperimentID FROM Experiments WHERE UserDefinedID = ?", (user_id,))
            experiment_id = cursor.fetchone()[0]

            for param_name, param_value in parameters.items():
                cursor.execute("INSERT INTO ExperimentParameters (ExperimentID, ParameterName, ParamValueTxt) VALUES (?, ?, ?)",
                               (experiment_id, param_name, param_value))

            cursor.execute("EXEC SpatialEvo_NewExperiment @UserExpID = ?, @NewPlateBC = ?, @ExpandPlateBC = ?",
                           user_id, self.barcode1, self.barcode2)
            result = cursor.fetchone()
            if result and "DATABASE ERROR" in str(result[0]):
                log(f"Stored proc error: {result[0]}")
                sys.exit(1)

            plate_id, cytomat_pos = result
            log(f"PlateID: {plate_id}, Cytomat Position: {cytomat_pos}")

            # Fixed SQL query - barcode2 should use self.barcode2
            cursor.execute("SELECT dbo.QueryCytomatPosition(?)", (self.barcode2,))
            expansion_plate_cytomatPos_result = cursor.fetchone()
            # Handle potential None result and extract just the position value
            if expansion_plate_cytomatPos_result:
                # The result is (2,) so we need the first element
                expansion_plate_cytomatPos = expansion_plate_cytomatPos_result[0]
                log(f"Expansion plate cytomat position: {expansion_plate_cytomatPos}")
            else:
                log("Warning: No expansion plate cytomat position found")
                expansion_plate_cytomatPos = None

            run_id = self.get_runID()

            # Write values to files
            try:
                # Write PlateID to file
                plate_id_path = f"C:\\EvoTaskFiles\\{run_id}_PlateID.txt"
                with open(plate_id_path, 'w') as f:
                    f.write(str(plate_id))
                log(f"Written PlateID to {plate_id_path}")

                # Write cytomat_pos to file
                cytomat_pos_path = f"C:\\EvoTaskFiles\\{run_id}_CytomatPos.txt"
                with open(cytomat_pos_path, 'w') as f:
                    f.write(str(cytomat_pos))
                log(f"Written Cytomat Position to {cytomat_pos_path}")

                # Write expansion_plate_cytomatPos to file
                if expansion_plate_cytomatPos is not None:
                    expansion_cytomat_path = f"C:\\EvoTaskFiles\\{run_id}_ExpansionCytomatPos.txt"
                    with open(expansion_cytomat_path, 'w') as f:
                        f.write(str(expansion_plate_cytomatPos))
                    log(f"Written Expansion Cytomat Position to {expansion_cytomat_path}")
                else:
                    log("Skipping expansion cytomat position file write - value is None")

            except IOError as e:
                log(f"Error writing to EvoTaskFiles: {e}")
                sys.exit(1)

            processed_df = self.process_excel_to_well_assignment(excel_file, run_id, plate_id)
            data = list(processed_df.itertuples(index=False, name=None))
            cursor.executemany(
                "INSERT INTO ImportPlatePattern (PlateID, WellID, RunID, WellAssign) VALUES (?, ?, ?, ?)", data)
            log(f"Inserted {len(data)} rows into ImportPlatePattern")

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
            sys.exit(1)

# === Main Application ===
if __name__ == "__main__":
    try:
        args = parse_args()
        barcode1 = args.barcode1
        barcode2 = args.barcode2

        root = tk.Tk()
        app = InputForm(root, barcode1, barcode2)
        root.mainloop()

    except Exception as e:
        log(f"Unhandled exception: {e}")
        sys.exit(1)
