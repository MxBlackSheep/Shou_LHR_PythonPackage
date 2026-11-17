# Python Modules for Champions_FL Workflow.

## Register an experiment before the first plate

From the repository root:

```powershell
uv run --locked python "VENUS Python Modules/Champions_FL/BeginNewExperiment/RegisterNewExperiment.py"
```

This program runs from source (no executable yet) and reuses the existing
experiment form and defaults. It registers the experiment, parameters and Excel
base layout in one transaction.
Selecting schedule `1` calls `dbo.ScheduleExperimentForImmediateExecution`, clearing
the other experiments' scheduling flags. The database must already contain
`dbo.ExperimentPlatePattern (ExperimentID, WellID, WellAssign)`.

Plate registration happens during the first Champions iteration when its barcode
is available. This program needs no barcodes and writes no task files. The existing
`StartNewExperiment_1.py`, executable and VENUS method are unchanged.

The opt-in database check uses Windows sign-in to `.\HAMILTON`, accepts only
`EvoYeast_check`, creates a sample Excel layout and rolls back its records and
scheduling changes. Run it separately from the offline suite:

```powershell
$env:HAMILTON_EXPERIMENT_ENTRY_SQL_TESTS = '1'
uv run --locked python -m unittest discover -s tests -p test_experiment_entry_sql.py -v
Remove-Item Env:HAMILTON_EXPERIMENT_ENTRY_SQL_TESTS
```

## 20260915

- Champions propagation selection, fluorescence calculations, deterministic well
  allocation and transfer volumes now run in Python. Supported fractions are
  25%, 50%, and 100%, with exact splits and one destination plate per invocation.
- Biological records commit transactionally using existing tables; no migration
  is required. Repeat calls after commit are rejected. Export failures after
  commit require manual reconciliation. See
  [propagation deployment and recovery](../../docs/champions-propagation.md).

## 20260910

- The nine Python files retain their workflow logic, argument parsing, GUI and
  entry points. They import reusable utilities from `hamilton_workflows` under `src`.
- Shared logging, SQL connections/lookups, Gen5 parsing, fluorescence simulation,
  BCP imports, Excel assignments and task/sequence writers have been extracted.
- Existing executable names, CLI arguments, SQL procedure calls and output file
  contracts are retained. Imports no longer start workflows.
- Experiment creation, plate selection, propagation rules, channel selection and
  transaction boundaries remain visible in the original scripts.
- See the [shared utilities](../../src/hamilton_workflows/) and the
  [setup, tests and build commands](../../README.md).

## 20251017

- Replace StartNewExperiment_SimulationFlourscent and ContinoueOnGoingExperiment_SimulationFlourscent with a uniformed SendGFPAndRFPToDB.
- This will allow Champions_FL_Python to run in three different modes: Simulation Mode, Test-Mode and Actual Experiment. 
- Only tested in simulation mode yet (20251016)

### Modules Detail and Usage

... To be Completed ...

## 20250826

- Initial commit to use python modules.
- Developed test modules for running in simulation mode. 

### Modules Detail and Usage

... To be Completed ...

