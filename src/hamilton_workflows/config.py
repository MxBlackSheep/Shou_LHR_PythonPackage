"""Robot defaults, with optional overrides for another installation or tests."""

from dataclasses import dataclass, field
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    odbc_driver: str = "ODBC Driver 11 for SQL Server"
    server: str = r"LOCALHOST\HAMILTON"
    database: str = "EvoYeast"
    username: str = "Hamilton"
    # Retained for compatibility with the existing robot installation.
    password: str = field(default="mkdpw:V43", repr=False)
    bcp_server: str = r"HAMILTON-PC\HAMILTON"
    log_dir: Path = Path(r"C:\Python Log")
    task_dir: Path = Path(r"C:\EvoTaskFiles")
    gen5_dir: Path = Path(r"C:\Program Files\HAMILTON\Gen5Data")
    layout_path: str = (
        r"C:\PROGRAM FILES\HAMILTON\METHODS\LABPROTOCOLS\EXPERIMENTS\DECKS"
        r"\SPATIALEVOLUTION3OD384WELL.LAY"
    )

    @classmethod
    def from_env(cls):
        """Read HAMILTON_<FIELD> overrides at launch, never at import time."""
        values = {}
        for name in cls.__dataclass_fields__:
            value = os.environ.get(f"HAMILTON_{name.upper()}")
            if value is not None:
                values[name] = Path(value) if name.endswith("_dir") else value
        return cls(**values)
