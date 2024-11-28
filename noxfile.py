import nox

# imports all nox task provided by the toolbox
from exasol.toolbox.nox.tasks import *
from noxconfig import PROJECT_CONFIG

# default actions to be run if nothing is explicitly specified with the -s option
nox.options.sessions = ["project:fix"]

def _coverage(session, config, context) -> None:
    # Overrides default implementation of PTB 
    # in order to only run unit tests for coverage
    from exasol.toolbox.nox._test import _unit_tests
    command = ["poetry", "run", "coverage", "report", "-m"]
    coverage_file = config.root / ".coverage"
    coverage_file.unlink(missing_ok=True)
    _unit_tests(session, config, context)
    session.run(*command)

@nox.session(name="test:coverage", python=False)
def coverage(session) -> None:
    from exasol.toolbox.nox._shared import _context
    """Runs all tests (unit + integration) and reports the code coverage"""
    context = _context(session, coverage=True)
    _coverage(session, PROJECT_CONFIG, context)

