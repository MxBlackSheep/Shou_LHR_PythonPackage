"""Stage and publish aligned VENUS outputs from an immutable transfer plan."""

from contextlib import contextmanager
import os
from pathlib import Path
from tempfile import TemporaryDirectory

from .propagation import validate_plan
from .task_files import task_path, write_hamilton_sequence, write_plain_lines


def write_propagation_files(plan, directory):
    validate_plan(plan)
    run_id = plan["snapshot"]["context"]["run_id"]
    rows = plan["transfers"]
    export = plan["export"]
    paths = []
    for suffix, field in (("CytomatPos", "cytomat_position"), ("CultureVol", "culture_volume"),
                          ("MediaVol", "media_volume")):
        path = task_path(directory, run_id, suffix)
        write_plain_lines(path, [row[field] for row in rows])
        paths.append(path)
    for suffix, field, labware, sequence in (
        ("SpillOverPlate_Positions", "destination_well", "spillover_labware", "seqSpillOverPlate"),
        ("SpatialOverPlate_Positions", "source_well", "source_labware", "seqEvoSrcPlate"),
    ):
        path = task_path(directory, run_id, suffix)
        write_hamilton_sequence(path, [row[field] for row in rows], export[labware], sequence, export["layout_path"])
        paths.append(path)
    return paths


@contextmanager
def stage_files(plan, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    # Same filesystem as the final outputs, so each replacement is atomic.
    with TemporaryDirectory(prefix=".champions-stage-", dir=directory) as temporary:
        yield write_propagation_files(plan, temporary)


def publish_files(paths, directory):
    # The bundle is not atomic. VENUS must honor the process exit code. Once SQL
    # has committed, publication failures require manual reconciliation.
    for path in paths:
        os.replace(path, Path(directory) / path.name)
