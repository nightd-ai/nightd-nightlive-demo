import csv
import yaml

from pathlib import Path

from ..utils import memory
from . import Status
from . import Step


def run(tracer):
    path = memory.PATH / f"{Step.STEP_3.value}.yml"
    data = yaml.safe_load(path.read_text())

    status = execute_plan(tracer, data["plan"])

    if status == Status.SUCCESS:
        memory.record(Step.STEP_4, {"constraints": data["constraints"]})

    return status


def execute_plan(tracer, plan):
    for edit in plan:
        path = Path(edit["file"])

        if not path.exists():
            tracer.trace("The file is invalid", **edit)

            return Status.FAILURE

        with open(path) as file:
            reader = csv.DictReader(file)

            rows = list(reader)
            columns = reader.fieldnames

        if edit["row"] not in range(0, len(rows)):
            tracer.trace("The row is invalid.", **edit)

            return Status.FAILURE

        if edit["column"] not in columns:
            tracer.trace("The column is invalid.", **edit)

            return Status.FAILURE

        rows[edit["row"]][edit["column"]] = edit["value"]

        with open(path, "w") as file:
            writer = csv.DictWriter(file, fieldnames=columns)

            writer.writeheader()
            writer.writerows(rows)

    return Status.SUCCESS
