import yaml

from pathlib import Path


PATH = Path(__file__).parent.parent / "memory"
PATH.mkdir(exist_ok=True)


def record(step, data):
    State(step).record(data)
    Constraints().record(data.get("constraints", []))
    Lessons().record(data.get("lessons", []))


class State:
    def __init__(self, step):
        self.path = PATH / f"{step.value}.yml"

    def record(self, data):
        state = {}

        for key in data:
            if key not in ("status", "constraints", "lessons"):
                state[key] = data[key]

        self.path.write_text(yaml.dump(state, default_flow_style=False, sort_keys=True))


class Constraints:
    def __init__(self):
        self.path = PATH / "constraints.yml"

    def record(self, other_constraints):
        constraints = {"constraints": []}

        if self.path.exists():
            constraints = yaml.safe_load(self.path.read_text())

        for other_constraint in other_constraints:
            if constraint := Constraints.search(constraints, other_constraint):
                constraint["rules"].extend(other_constraint["rules"])
            else:
                constraints["constraints"].append(other_constraint)
                constraints["constraints"] = sorted(constraints["constraints"], key=Constraints.tuplify)

        self.path.write_text(yaml.dump(constraints, default_flow_style=False, sort_keys=True))

    @staticmethod
    def search(constraints, other_constraint):
        for constraint in constraints["constraints"]:
            if Constraints.tuplify(constraint) == Constraints.tuplify(other_constraint):
                return constraint

    @staticmethod
    def tuplify(constraint):
        return (constraint["file"], constraint["row"], constraint["column"])


class Lessons:
    def __init__(self):
        self.path = PATH / "lessons.yml"

    def record(self, other_lessons):
        lessons = {"lessons": []}

        if self.path.exists():
            lessons = yaml.safe_load(self.path.read_text())

        for other_lesson in other_lessons:
            lessons["lessons"].append(other_lesson)

        self.path.write_text(yaml.dump(lessons, default_flow_style=False, sort_keys=True))
