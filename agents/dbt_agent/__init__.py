from pathlib import Path


def _find_project_path():
    path = Path(__file__).resolve().parent

    while path != path.parent:
        if (path / "dbt_project.yml").exists():
            return path

        path = path.parent

    raise FileNotFoundError("Could not find dbt_project.yml.")


PROJECT_PATH = _find_project_path()
PACKAGE_PATH = Path(__file__).resolve().parent
STORAGE_PATH = PACKAGE_PATH / "memory"
STORAGE_PATH.mkdir(exist_ok=True)
