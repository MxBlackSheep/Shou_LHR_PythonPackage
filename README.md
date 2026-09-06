# Introduction

This REPO contains Python code developed for Shou Group Liquid Handling Robotic to perform Evolution Experiment.

# Python environment

Development and builds use **CPython 3.12 on 64-bit Windows**, managed with
[uv](https://docs.astral.sh/uv/getting-started/installation/). Install uv using
its official Windows instructions, then run the commands below from the repository
root in PowerShell 7. No global pip installs or manual environment activation are
needed after uv is installed.

If you have the old Python 3.8 / 32-bit `.venv`, deactivate it and close IDE
processes using it. Preserve it before the first sync (choose another backup name
if this one already exists):

```powershell
Rename-Item -LiteralPath .venv -NewName .venv-py38-backup
```

For a fresh checkout, skip that rename. Create the new environment with:

```powershell
uv python install
uv sync --locked
uv run --locked python scripts/check_environment.py
```

`.python-version` selects the 64-bit Windows CPython 3.12 interpreter;
`pyproject.toml` restricts the Python minor version and Windows architecture.
`uv.lock` records exact dependency versions. Commit all three files together.
Set the IDE interpreter to `.venv\Scripts\python.exe` after syncing.

Runtime dependencies include pyodbc, pandas, pyserial and requests, plus openpyxl
for `.xlsx` and xlrd for legacy `.xls` input. PyInstaller is in the default `dev`
dependency group. Tkinter comes with the Python runtime, not a pip package.
The scripts remain standalone; extracting a shared Python package is a separate change.

To intentionally update dependencies, run `uv lock --upgrade`, then
`uv sync --locked` and repeat validation before reviewing the lockfile diff.
Normal setup and builds should use `--locked` to avoid silently changing resolution.

# Build single-file executables

The utilities are packaged as standalone executables so Hamilton/VENUS can call
them directly. Single-file deployment is retained; the robot PC does not need uv
or a separately installed Python runtime to run these executables.

Use the locked environment and an actual source path, for example:

```powershell
uv run --locked python scripts/check_environment.py
uv run --locked pyinstaller --clean --noconfirm --onefile --noconsole --specpath build/specs "VENUS Python Modules/Champions_FL/BeginNewExperiment/StartNewExperiment_1.py"
```

The new executable is `dist\StartNewExperiment_1.exe`. Replace the source path to
build another utility. `--noconfirm` replaces that build's existing output under
`dist`; it does not deploy it. Local legacy `.spec` files can contain stale paths;
the command above generates a fresh specification under ignored `build/specs`.
Build on Windows with the verified 64-bit interpreter. Existing executables in
the repository are historical artifacts and are **not** converted by `uv sync`.

# Migration validation

`scripts/check_environment.py` verifies the runtime version and bitness, dependency
imports, Tcl initialization, an in-memory Excel round-trip and well assignment,
and compilation of Python sources without executing the workflow scripts. It also
lists the ODBC drivers visible to the 64-bit process. It does not connect to SQL
Server, open serial ports, submit experiments, or exercise a GUI window.

Before replacing executables on the robot PC:

- Confirm the PC runs a Windows version supported by Python 3.12 and the locked
  dependencies. Match the **64-bit ODBC driver** to the driver name used in the
  scripts (currently `ODBC Driver 11 for SQL Server`). A 32-bit-only driver will
  not work with the new executables. Changing SQL driver versions is separate
  from this migration and may require connection-string changes.
- Verify SQL Server access, `bcp` availability for workflows that use it, COM-port
  configuration, and permissions for the existing log and task-file paths.
- Test the setup GUI with representative Excel files against a test database;
  compare database changes, output files and exit codes with the old build.
- Validate serial commands on the appropriate test hardware before production use.
- Measure first and repeated GUI launches on the robot PC. This migration alone
  does not promise a startup-speed improvement or change one-file extraction.

Keep the previous deployed executables until these checks pass so they can be
restored if needed. A file named `test_*.py` under the workflow directories can
write to the database; do not treat those scripts as an isolated unit-test suite.

# Python module for Hamilton VENUS method

- [Champions_FL](./VENUS%20Python%20Modules/Champions_FL/Champions_FL.md)

# Python Code to Control Hardware

- [Teleshake](./Labwares/Teleshake/Teleshake.md)

# VENUS Code Backup

- [Champions_FL_Python](./VENUS-Method/Champions_FL_Python/)
- [ChamFL_Flourscent](./VENUS-Method/ChamFL_Flourscent/)
