from enum import Enum
from pathlib import Path


class Step(Enum):
    STEP_1 = "step_1"
    STEP_2 = "step_2"
    STEP_3 = "step_3"
    STEP_4 = "step_4"

    @staticmethod
    def load(filename):
        filepath = Path(__file__).parent / filename

        return filepath.read_text()


class Status(Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
