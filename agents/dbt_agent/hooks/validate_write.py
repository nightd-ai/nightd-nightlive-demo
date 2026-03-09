from . import block


async def validate_write(input_data, tool_use_id, context):
    return block(input_data.get("hook_event_name", "Write"), "Write is currently not allowed.")
