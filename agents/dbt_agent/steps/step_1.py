import json

from dbt.cli.main import dbtRunner
from pathlib import Path

from ..utils import memory
from . import Status
from . import Step


PATH = Path(__file__).parent.parent.parent.parent


def run(tracer):
    result = dbtRunner().invoke(["--log-level", "none", "build"])

    if result.success:
        return Status.SUCCESS

    if result.exception:
        raise result.exception

    memory.record(Step.STEP_1, {"failure": extract_failure(result.result.results)})

    return Status.FAILURE


def extract_failure(results):
    manifest = json.loads((PATH / "target" / "manifest.json").read_text())

    for result in results:
        if result.status in ("error", "fail"):
            return {
                "status": result.status,
                "message": result.message,
                "node": {
                    "name": result.node.name,
                    "unique_id": result.node.unique_id,
                    "resource_type": str(result.node.resource_type),
                    "original_file_path": result.node.original_file_path,
                    "compiled_path": result.node.compiled_path,
                    "depends_on": DependencyResolver(manifest).resolve(result.node),
                },
            }

    return {}


class DependencyResolver:
    def __init__(self, manifest):
        self.manifest = manifest
        self.dependencies = []

    def resolve(self, node):
        self.resolve_dependencies({"depends_on": {"nodes": node.depends_on.nodes}})

        return self.dependencies

    def resolve_dependencies(self, node):
        if "depends_on" in node and "nodes" in node["depends_on"]:
            for unique_id in node["depends_on"]["nodes"]:
                if unique_id in self.manifest["nodes"]:
                    self.resolve_dependency(self.manifest["nodes"][unique_id])
                elif unique_id in self.manifest["sources"]:
                    self.resolve_dependency(self.manifest["sources"][unique_id])

    def resolve_dependency(self, node):
        for dependency in self.dependencies:
            if node["unique_id"] == dependency["unique_id"]:
                return

        dependency = {
            "name": node["name"],
            "unique_id": node["unique_id"],
            "resource_type": node["resource_type"],
            "original_file_path": node["original_file_path"],
        }

        if "compiled_path" in node:
            dependency["compiled_path"] = node["compiled_path"]

        self.dependencies.append(dependency)
        self.resolve_dependencies(node)
