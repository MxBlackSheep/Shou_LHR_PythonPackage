# Python Modules for Champions_FL Workflow.

## 20260910

- The eight Python files retain their workflow logic, argument parsing, GUI and
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

