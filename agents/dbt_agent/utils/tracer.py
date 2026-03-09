import logging
import time
import yaml

from claude_agent_sdk.types import AssistantMessage
from claude_agent_sdk.types import ResultMessage
from claude_agent_sdk.types import TextBlock
from datetime import timedelta


logger = logging.getLogger("dbt_agent")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)
logger.propagate = False


class Tracer:
    def __init__(self):
        self.step_count = 0
        self.total_cost = 0.0
        self.started_at = time.monotonic()

    def step(self, step):
        self.step_count += 1

        return StepTracer(step, self)


class StepTracer:
    def __init__(self, step, tracer):
        self.step = step
        self.tracer = tracer
        self.started_at = None

    def __enter__(self):
        self.started_at = time.monotonic()

        trace = {
            "step": self.step.value,
            "step_count": self.tracer.step_count,
        }

        logger.info(yaml.dump({"trace": trace}, default_flow_style=False, sort_keys=False).strip())

        return self

    def __exit__(self, *args, **kwargs):
        trace = {
            "step": self.step.value,
            "step_count": self.tracer.step_count,
            "total_cost": f"${self.tracer.total_cost:.4f}",
            "step_runtime": str(timedelta(seconds=int(time.monotonic() - self.started_at))),
            "total_runtime": str(timedelta(seconds=int(time.monotonic() - self.tracer.started_at))),
        }

        logger.info(yaml.dump({"trace": trace}, default_flow_style=False, sort_keys=False).strip())

        return False

    def trace(self, message, **kwargs):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    self._trace(block.text)
        elif isinstance(message, ResultMessage):
            self.tracer.total_cost += message.total_cost_usd or 0.0
        elif isinstance(message, str):
            self._trace(message, **kwargs)

    def _trace(self, message, **kwargs):
        trace = {
            "step": self.step.value,
            "step_count": self.tracer.step_count,
            "step_runtime": str(timedelta(seconds=int(time.monotonic() - self.started_at))),
            "message": message,
            **kwargs,
        }

        logger.info(yaml.dump({"trace": trace}, default_flow_style=False, sort_keys=False).strip())
