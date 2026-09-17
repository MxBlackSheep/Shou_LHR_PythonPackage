# Champions Python propagation

The setup GUI and propagation backend accept only 25%, 50%, and 100%. They
produce exactly four, two, or one child per selected parent. The eligible source
population must be divisible by the split count. Existing experiments containing
75% or another unsupported value are rejected; no parameter is silently changed.
No database migration or new table is required.

## Calculation and allocation

The source barcode must identify a non-discarded plate in the single scheduled
experiment's chain. Eligible cultures have a latest history record with
`Status_mature=0` and a timestamp before the selected Hamilton run. NULL-well
records used to link plates are excluded. Missing/ambiguous history and missing,
duplicate, or nonfinite eligible fluorescence readings cause failure.

Both fluorescence channels are clamped to their legacy baseline of 10. The
legacy coefficients remain fixed at 4669.1, 773.1, 2262.3, and 305.0. Individual
unmixed OD components may be negative; they are not independently clamped. Total
OD is their sum. Both newly recorded parent and child total OD now use this sum,
instead of the legacy procedure's zero-valued OD field. Historical OD is unchanged.

Parents are ranked by descending combined OD, then numeric column, row, and
culture ID. Selected parents are visited in numeric column/row order each round.
Available children come from `dbo.UnoccupiedWellsInPlatesChain`, retaining the
ancestor culture-well pattern and existing occupancy. The source plate is
excluded. Destinations are ordered by plate ID, numeric column, and row; the
first eligible-population-count destinations are assigned once each.

Each round gives each selected parent one child. For example, two parents and
two rounds give `A1 -> A1`, `B1 -> B1`, `A1 -> C1`, `B1 -> D1` when those are the
first available child wells. Parent order resets between rounds. Parent, child,
plate, and volume always remain one transfer record.

Allocation fails if these first destinations span multiple plates. It does not
skip a partially occupied earlier plate in favor of a later plate. The selected
plate must have one valid Cytomat position and a barcode. Allocation also fails
on insufficient capacity, invalid wells, duplicate destinations, or ID conflicts.

For split count `k`, target volume `V`, target inoculation OD `I`, and total source
OD `D`, culture volume is `min(I * V / D, V / 3 / k)` and media is `V - culture`.
The sampling dilution factor remains `V / (V - V_OD_Sample)`. Summed culture
withdrawals from each parent must not exceed `V - V_OD_Sample`. GUI calibration
parameters are not newly substituted for the legacy fixed coefficients.

## Transaction and recovery

The workflow reads source-specific data directly. It no longer invokes
`Champions_CommencePropagationFl`, imports a subset with BCP, or writes a subset
task file. Existing stored procedures remain available to other workflows.

An experiment application lock serializes planning. Serializable reads and
destination checks guard against concurrent occupancy changes, including legacy
writers that do not use application locks. Inputs are reloaded before insertion.
Parent history, children, child history, and lineage commit together using the
existing tables. The calculated plan is held in memory; there is no database
journal or local recovery-file dependency.

All five files are staged beside the destination directory before the database
commit. After the commit each file is atomically replaced. The five-file bundle
is **not** atomic: VENUS must stop on a nonzero process exit code and must not
read the files before successful process completion. The workflow holds a
session application lock through publication to serialize the shared run-file
names. Separate VENUS consumers must still coordinate their physical execution.

Failure handling depends on whether the biological transaction committed:

- If staging or persistence fails before commit, SQL rolls back and temporary
  output files are removed. After resolving the failure, the same explicit
  `--run-id` can be retried if its source records remain eligible.
- If SQL commits but publication fails, the script exits with an explicit manual
  reconciliation message. Some output files may have been replaced. Stop VENUS
  and reconcile the existing database records, output files and actual liquid
  transfers before any continuation. There is no automatic export recovery.
- A repeated invocation after a successful commit is rejected by the existing
  source-history timestamp checks, before any new writes or file exports. This
  also protects against competing callers for the same source/run.
- If a connection fails during commit, treat the commit outcome as uncertain and
  inspect the database and robot state. Do not switch to a newer run ID, delete
  history, or use the legacy procedure to bypass the repeat-call guard.

The five-file bundle and SQL transaction cannot be committed atomically.
Physical completion and restart decisions remain with VENUS and the operator.
The workflow neither changes existing schemas nor reads/writes a plan-journal
table that might have been created by an earlier build.

## CLI and output contract

The three positional arguments are unchanged. `--run-id` selects a specific
Hamilton run; omitting it retains the latest-run lookup. Prefer the explicit
option for diagnosis and recovery.

```powershell
.venv/Scripts/python.exe 'VENUS Python Modules/Champions_FL/ContinueOnGoingExperiment/ContinueOngoingExperiment_platechain.py' 'SOURCE_BARCODE' 'SOURCE_LABWARE' 'CHILD_LABWARE' --run-id 'RUN_ID' --dry-run
```

`--dry-run` validates and prints the serialized plan and includes it in the
workflow log. It does not commit database data or create VENUS task files.
A windowless executable has no console;
read its workflow log for the report, or use the Python CLI above.

Output filenames remain:

| Filename suffix | Contents |
| --- | --- |
| `_CytomatPos.txt` | One destination position per transfer |
| `_CultureVol.txt` | One culture volume per transfer |
| `_MediaVol.txt` | One media volume per transfer |
| `_SpillOverPlate_Positions.txt` | `seqSpillOverPlate`, supplied child labware |
| `_SpatialOverPlate_Positions.txt` | `seqEvoSrcPlate`, supplied source labware |

Each is prefixed with the run ID. Sequence headers, LF line endings and plain
volume/position-file formatting match the previous Python writer. Some archived
VENUS `.sub` files in this repository reference older sequence suffixes without
`_Positions`; confirm the deployed consumer uses the five names above before
commissioning. This change does not alter archived VENUS methods.

## Builds and deployment

The staged review bundle is `dist/champions-propagation-review/`. It contains the
two executables, this guide, validation results, and SHA-256 checksums. Builds use Python
3.12, 64-bit Windows and PyInstaller. The propagation executable deliberately
retains the deployed spelling `ContinueOnGoingExperiment_platechain.exe`.

No SQL migration is needed. The runtime account needs the existing source-table
reads, biological-table inserts, and SQL Server application-lock access. No
additional table or procedure is installed. Any previously created journal table
is ignored and is not automatically dropped.

With the robot idle, back up the currently deployed executables and install the reviewed builds to:

- `C:\Program Files\HAMILTON\Python\Champions_FL\BeginNewExperiment\StartNewExperiment_1.exe`
- `C:\Program Files\HAMILTON\Python\Champions_FL\ContinueOnGoingExperiment\ContinueOnGoingExperiment_platechain.exe`

Confirm the setup dropdown, run a dry-run on appropriate unprocessed test inputs,
and commission with a controlled VENUS simulation before live liquid handling.
Installing these binaries is separate from building the review bundle. Do not
switch to the legacy propagation caller in the middle of a committed run to
bypass repeat-call protection.

Rebuild either executable using the README PyInstaller command with the matching
`--name`, `--distpath dist/champions-propagation-review`, and source path.
Before building the setup GUI, confirm Tcl initializes in the build environment:

```powershell
.venv/Scripts/python.exe -c "import tkinter; print(tkinter.Tcl().eval('info patchlevel'))"
```

PyInstaller can report a successful build while omitting tkinter if Tcl cannot
initialize. Treat its `tkinter installation is broken` warning as a failed GUI
build. During validation on this host, Tcl initialized normally outside the
restricted sandbox, so the setup executable was rebuilt there with Tcl/Tk included.

## Validation

The default suite uses mocks and temporary files:

```powershell
.venv/Scripts/python.exe -B -m unittest discover -s tests -v
```

Opt-in integration tests require permission to create/drop a test database:

```powershell
$env:HAMILTON_PROPAGATION_SQL_TESTS = '1'
$env:HAMILTON_PROPAGATION_EXE = (Resolve-Path 'dist/champions-propagation-review/ContinueOnGoingExperiment_platechain.exe').Path
.venv/Scripts/python.exe -B -m unittest discover -s tests -v
Remove-Item Env:HAMILTON_PROPAGATION_SQL_TESTS
Remove-Item Env:HAMILTON_PROPAGATION_EXE
```

These tests create a uniquely named `CodexChampionsTest_<uuid>` database, copy
only the three read-only chain-function definitions, and seed synthetic cultures.
They read an existing Hamilton run timestamp without modifying HamiltonVectorDB.
They exercise SQL expressions, exact population sizes, transaction rollback,
repeat-call rejection, and competing callers, then drop the generated database.
The fixture contains only existing biological tables and deliberately omits the
former plan-journal table.
No live propagation procedure is executed. If interrupted externally before
cleanup, only the logged/generated test database should be considered for removal.
