"""VENUS text and Hamilton sequence formats; preserve row order and line endings."""

import csv
import os
from pathlib import Path


def task_path(directory, run_id, suffix):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{run_id}_{suffix}.txt"


def add_test_suffix_preserve_ext(path):
    root, ext = os.path.splitext(path)
    return f"{root}_test{ext}"


def write_text(path, value):
    with open(path, "w", encoding="utf-8") as output:
        output.write(str(value))


def write_plain_lines(path, values, *, trailing_newline=False):
    lines = [str(value) for value in values]
    text = "\n".join(lines)
    if lines and trailing_newline:
        text += "\n"
    write_text(path, text)


def write_tsv_rows(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as output:
        csv.writer(output, delimiter="\t").writerows(rows)


def write_hamilton_sequence(path, positions, labware, sequence, layout_path):
    # VENUS expects this literal header and LF endings (not CSV's default CRLF).
    with open(path, "w", newline="", encoding="utf-8") as output:
        output.write("Id,Layout,Sequence,Labware,Position\n")
        for index, position in enumerate(positions, 1):
            output.write(f"{index},{layout_path},{sequence},{labware},{position}\n")
