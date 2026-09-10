# Shou Group Hamilton workflows

Python workflows and reusable utilities for Shou Group evolution experiments on
Hamilton liquid-handling robots.

## Setup with uv

Development and executable builds use **Python 3.12 on 64-bit Windows** and
**PowerShell 7**. Install [uv](https://docs.astral.sh/uv/getting-started/installation/),
then run these commands from the repository root:

```powershell
uv python install
uv sync --locked
```

uv installs the project-selected Python version, creates `.venv`, and installs
the locked dependencies, development tools and shared `hamilton_workflows` package.
Use `uv run --locked` for project commands; no manual environment activation is
needed. Set your IDE interpreter to `.venv\Scripts\python.exe`.

Running workflows on a robot also requires its SQL Server, hardware and task-file
configuration. Database workflows default to **ODBC Driver 11 for SQL Server**;
BCP workflows require the `bcp` command. Champions defaults and environment-variable
overrides are defined in [config.py](src/hamilton_workflows/config.py).

## Run workflows

Run a script with its required arguments, for example:

```powershell
uv run --locked python "VENUS Python Modules/Champions_FL/BeginNewExperiment/StartNewExperiment_1.py" "NEW_PLATE_BARCODE" "EXPANSION_PLATE_BARCODE"
```

Replace the barcode placeholders with your plate barcodes. This opens the experiment
setup dialog; submitting it creates experiment records. Each workflow keeps its
arguments and experiment logic in its original Python file and imports common
utilities from [hamilton_workflows](src/hamilton_workflows/).

## Tests

```powershell
uv run --locked python -m unittest discover -s tests -v
```

The isolated suite uses temporary files and mocked database/BCP calls. Keep
`-s tests`: test-named utilities under the workflow directories can write to SQL Server.

## Build executables

Build a workflow directly with PyInstaller:

```powershell
uv run --locked pyinstaller --clean --noconfirm --onefile --noconsole --paths src --specpath build/specs "VENUS Python Modules/Champions_FL/BeginNewExperiment/StartNewExperiment_1.py"
```

This creates `dist\StartNewExperiment_1.exe`. Replace the source path to build another
workflow or hardware utility. `--noconfirm` replaces an existing build output.
Each executable bundles Python and its dependencies, including any shared utilities
it imports, so the robot PC does not need uv or a separate Python installation.

Deploy the executable to the path expected by the VENUS method. In particular,
`SendGFPAndRFPToDB.exe` is called from `Champions_FL\ContinueOnGoingExperiment`.
Building does not deploy the files.

## Update dependencies

For an intentional dependency upgrade:

```powershell
uv lock --upgrade
uv sync --locked
uv run --locked python -m unittest discover -s tests -v
```

Review and commit dependency changes in `pyproject.toml` and `uv.lock` together.
Normal setup, runs and builds use `--locked` to preserve the recorded versions.

## Project reference

- [Shared utilities](src/hamilton_workflows/)
- [Champions workflow notes](VENUS%20Python%20Modules/Champions_FL/Champions_FL.md)
- [Teleshake hardware control](Labwares/Teleshake/Teleshake.md)
- VENUS method backups: [Champions_FL_Python](VENUS-Method/Champions_FL_Python/),
  [ChamFL_Flourscent](VENUS-Method/ChamFL_Flourscent/)
