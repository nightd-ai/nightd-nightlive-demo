import textwrap


def block(event, reason):
    message = """
        If this operation is essential to completing your task, record it
        as a lesson in the `lessons` array so the hook can be improved.
    """

    return {
        "systemMessage": textwrap.dedent(message).strip(),
        "hookSpecificOutput": {
            "hookEventName": event,
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        },
    }
