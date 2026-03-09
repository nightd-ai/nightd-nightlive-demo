from . import block


async def validate_edit(input_data, tool_use_id, context):
    return block(input_data.get("hook_event_name", "Edit"), "Edit is currently not allowed.")
