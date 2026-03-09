from pathlib import Path

from .. import PROJECT_PATH
from .. import STORAGE_PATH
from . import block


PREFIXES = [
    "models/",
    "seeds/",
    "tests/",
    str(STORAGE_PATH.resolve().relative_to(PROJECT_PATH)),
]


async def validate_read(input_data, tool_use_id, context):
    if "tool_input" not in input_data:
        return {}

    if "path" not in input_data["tool_input"]:
        return {}

    path = Path(input_data["tool_input"]["path"].strip().strip('"').strip("'")).resolve()

    if not path.is_relative_to(PROJECT_PATH):
        return block(input_data.get("hook_event_name", "Read"), f"Grep is restricted to {PREFIXES}. Got: {path}.")

    if any(str(path.relative_to(PROJECT_PATH)).startswith(prefix) for prefix in PREFIXES):
        return {}

    return block(input_data.get("hook_event_name", "Read"), f"Grep is restricted to {PREFIXES}. Got: {path}.")
