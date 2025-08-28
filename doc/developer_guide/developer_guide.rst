🛠 Developer Guide
==================

Setting up the workspace
*************************

Check out the Project
---------------------

.. code-block:: shell

    git checkout git@github.com:exasol/bucketfs-python.git

Install project dependencies
----------------------------

.. code-block:: shell

    poetry install

Setup pre-commit hook(s)
------------------------

.. code-block:: shell

    poetry run -- pre-commit install

Tests
*****

Pytest Plugins
--------------

BFSPY declares a dependency to pytest plugin ``pytest-exasol-backend`` which is
maintained in GitHub repository `pytest-plugins/pytest_backend
<https://github.com/exasol/pytest-plugins/tree/main/pytest-backend/>`_.  This
plugin makes additional fixtures available that are used in the saas
integration tests of BFSPY, see files in folder `test_service_saas.py
<https://github.com/exasol/bucketfs-python/blob/main/test/integration/test_service_saas.py>`_.

Running Tests in CI Builds
--------------------------

The test cases in BFSPY are separated in two groups.

+-----------------------------+-----------------------------------------+--------------------------------+
| Group                       | Execution                               | Name of gating GitHub workflow |
+=============================+=========================================+================================+
| G1) Fast and cheap tests    | On each push to your development branch | Gate 1 - Regular CI            |
+-----------------------------+-----------------------------------------+--------------------------------+
| G2) Slow or expensive tests | Only on manual approval, see below      | Gate 2 - Allow Merge           |
+-----------------------------+-----------------------------------------+--------------------------------+

This enables fast development cycles while still protecting the main branch
against build failures.

For BFSPY group G2 particularly contains the tests involving Exasol SaaS
infrastructure which are creating costs for the database instances temporarily
created during test execution.

Group G2 is guarded by a dedicated `GitHub Enviroment
<https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments>`_
requiring **manual approval** before these tests are executed.

Each of the groups results in a final gating GitHub workflow job that is added to the branch protection of branch ``main``.

So in order to merge a branch to ``main`` branch, the tests of both groups need to be executed and to have terminated succesfully.

Approving Slow Tests
~~~~~~~~~~~~~~~~~~~~

To approve executing the tests in group G2

* Open your pull request in GitHub
* Scroll to section "Checks"
* Locate pending tasks, e.g. "Ask if Slow or Expensive Tests (e.g. SaaS) Should be Run"
* Click the link "Details" on the right-hand side
* Click "Review pending Deplopyments"
* Select the checkbox "slow-tests"
* Click the green button "Approve and deploy"

Preparing & Triggering a Release
********************************

The `exasol-toolbox` provides nox tasks to semi-automate the release process:

.. code-block:: python

    # prepare a release
    nox -s release:prepare -- --type {major,minor,patch}

    # trigger a release
    nox -s release:trigger

For further information, please refer to the `exasol-toolbox`'s page `How to Release
<https://exasol.github.io/python-toolbox/main/>`_.
