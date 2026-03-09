from pathlib import Path

from ..steps import PATH
from . import block


PREFIXES = [
    "models/",
    "seeds/",
    "tests/",
    str(PATH),
]


async def validate_grep(input_data, tool_use_id, context):
    if "tool_input" not in input_data:
        return {}

    if "path" not in input_data["tool_input"]:
        return {}

    path = Path(input_data["tool_input"]["path"].strip().strip('"').strip("'"))
    path = path.resolve().relative_to(Path(__file__).resolve().parents[3])

    if any(path.startswith(prefix) for prefix in PREFIXES):
        return {}

    return block(input_data.get("hook_event_name", "Grep"), f"Grep is restricted to {PREFIXES}. Got: {path}.")
