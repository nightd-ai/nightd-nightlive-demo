from . import block


async def validate_bash(input_data, tool_use_id, context):
    return block(input_data.get("hook_event_name", "Bash"), "Bash is currently not allowed.")
