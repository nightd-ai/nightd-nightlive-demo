import argparse
import asyncio

from .steps import Status
from .steps import Step
from .steps import step_1
from .steps import step_4
from .utils.runner import Runner
from .utils.tracer import Tracer


async def main(model):
    await StateMachine(model).run()


class StateMachine:
    def __init__(self, model):
        self.step = Step.STEP_1
        self.tracer = Tracer()
        self.runner = Runner(model)

    async def run(self):
        while self.step is not None:
            with self.tracer.step(self.step) as tracer:
                match self.step:
                    case Step.STEP_1:
                        status = step_1.run(tracer)

                    case Step.STEP_2 | Step.STEP_3:
                        status = await self.runner.run_step(self.step, tracer)

                    case Step.STEP_4:
                        status = step_4.run(tracer)

            self.transition(status)

    def transition(self, status):
        match (self.step, status):
            case (Step.STEP_1, Status.SUCCESS):
                self.step = None
            case (Step.STEP_1, Status.FAILURE):
                self.step = Step.STEP_2
            case (Step.STEP_2, Status.SUCCESS):
                self.step = Step.STEP_3
            case (Step.STEP_3, Status.SUCCESS):
                self.step = Step.STEP_4
            case (Step.STEP_4, Status.SUCCESS):
                self.step = Step.STEP_1
            case (_, Status.FAILURE):
                self.step = None


parser = argparse.ArgumentParser()
parser.add_argument("--model", default="sonnet")
args = parser.parse_args()

asyncio.run(main(args.model))
