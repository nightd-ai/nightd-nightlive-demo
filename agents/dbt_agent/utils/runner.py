import json
import textwrap

from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk import ClaudeSDKClient
from claude_agent_sdk import HookMatcher
from claude_agent_sdk.types import ResultMessage

from .. import STORAGE_PATH
from ..hooks.validate_bash import validate_bash
from ..hooks.validate_edit import validate_edit
from ..hooks.validate_glob import validate_glob
from ..hooks.validate_grep import validate_grep
from ..hooks.validate_read import validate_read
from ..hooks.validate_write import validate_write
from ..steps import Status
from ..steps import Step
from . import memory


class Runner:
    def __init__(self, model):
        self.model = model

    async def run_step(self, step, tracer):
        system_prompt = f"""
            You are a dbt data engineer.

            You must interpret the words "MUST", "MUST NOT", "SHALL", "SHOULD", and "MAY"
            in this document as described in RFC 2119.

            # Context

            The dbt project (macros, models, tests) is correct. Only the seed CSV files
            in `seeds/` may contain errors. When a test fails, the root cause is always
            in the seed data - never in the test logic or model SQL.

            # Guardrails

            - SHOULD NOT modify any files.
            - SHOULD NOT run any Bash commands.

            # Lessons

            Your output includes a `lessons` array. Use it to report suggestions for
            improving the process, prompts, or guardrails based on what you observed.

            ## Approved Tools

            ### Glob / Grep / Read

            - You MAY read dbt project files (models/, seeds/, tests/) and memory files
            ({STORAGE_PATH}/).
        """

        options = ClaudeAgentOptions(
            model=self.model,
            max_turns=99,
            max_budget_usd=5,
            system_prompt={
                "type": "preset",
                "preset": "claude_code",
                "append": textwrap.dedent(system_prompt).strip(),
            },
            setting_sources=["project"],
            permission_mode="bypassPermissions",
            tools={
                "type": "preset",
                "preset": "claude_code",
            },
            allowed_tools=[
                "Bash",
                "Edit",
                "Glob",
                "Grep",
                "Read",
                "Write",
            ],
            hooks={
                "PreToolUse": [
                    HookMatcher(matcher="Bash", hooks=[validate_bash]),
                    HookMatcher(matcher="Edit", hooks=[validate_edit]),
                    HookMatcher(matcher="Glob", hooks=[validate_glob]),
                    HookMatcher(matcher="Grep", hooks=[validate_grep]),
                    HookMatcher(matcher="Read", hooks=[validate_read]),
                    HookMatcher(matcher="Write", hooks=[validate_write]),
                ],
            },
            output_format={
                "type": "json_schema",
                "schema": json.loads(Step.load(f"{step.value}.json")),
            },
            stderr=tracer.trace,
        )

        async with ClaudeSDKClient(options) as client:
            await client.query(Step.load(f"{step.value}.md").format(path=STORAGE_PATH))

            async for message in client.receive_response():
                tracer.trace(message)

                if isinstance(message, ResultMessage) and message.structured_output:
                    memory.record(step, message.structured_output)

                    return Status(message.structured_output["status"])

        return Status.FAILURE
