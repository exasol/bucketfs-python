import subprocess
from pathlib import Path

import pytest


def find_script(script_name: str):
    current_path = Path(__file__).parent
    script_path = None
    while current_path != current_path.root:
        script_path = Path(current_path, script_name)
        if script_path.exists():
            break
        current_path = current_path.parent
    if script_path.exists():
        return script_path
    else:
        raise RuntimeError(f"Could not find {script_name}")


@pytest.fixture(scope="session")
def language_container():
    script_dir = find_script("build_language_container.sh")
    completed_process = subprocess.run(
        [script_dir], stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    output = completed_process.stdout.decode("UTF-8")
    print(output)
    completed_process.check_returncode()
    lines = output.splitlines()
    alter_session_selector = "ALTER SESSION SET SCRIPT_LANGUAGES='"
    alter_session = [line for line in lines if line.startswith(alter_session_selector)][
        0
    ]
    alter_session = alter_session[len(alter_session_selector) : -2]
    container_path_selector = "Cached container under "
    container_path = [
        line for line in lines if line.startswith(container_path_selector)
    ][0]
    container_path = container_path[len(container_path_selector) :]
    return {"container_path": container_path, "alter_session": alter_session}
