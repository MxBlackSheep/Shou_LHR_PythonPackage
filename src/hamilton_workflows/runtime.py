"""Timestamped file logging and process-boundary error handling."""

from datetime import datetime
from pathlib import Path
import sys


def discard_log(message):
    """Default log sink for reusable functions."""


class RunLogger:
    """A callable logger that opens files only when a message is written."""

    def __init__(self, directory, script_name=None):
        name = script_name or Path(sys.argv[0]).stem
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.path = Path(directory) / f"{name}_{timestamp}.log"

    def __call__(self, message):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.path.open("a", encoding="utf-8") as output:
            output.write(f"[{now}] {message}\n")


def run_logged(action, log):
    """Map workflow failures to VENUS exit code 1; success to 0."""
    try:
        action()
        return 0
    except Exception as error:
        log(f"Fatal error: {error}")
        return 1
