from pathlib import Path
from typing import Dict

from exasol_script_languages_container_tool.lib.tasks.build.docker_flavor_image_task import (
    DockerFlavorAnalyzeImageTask,
)


class AnalyzeRelease(DockerFlavorAnalyzeImageTask):
    def get_build_step(self) -> str:
        return "release"

    def requires_tasks(self):
        return {}

    def get_path_in_flavor(self):
        return "flavor_base"
