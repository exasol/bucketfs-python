🛠 Developer Guide
==================

Setting up the workspace
*************************

Checkout the Project
--------------------

.. code-block:: shell

    git checkout git@github.com:exasol/bucketfs-python.git

Install project dependencies
----------------------------

.. code-block:: shell

    poetry install

Setup pre-commit hook(s)
------------------------

.. code-block:: shell

    poetry run pre-commit install

Tests
*****

Pytest Plugins
--------------

BFSPY declares a dependency to pytest plugin ``pytest-exasol-saas`` which is
maintained in GitHub repository `pytest-plugins/pytest_saas
<https://github.com/exasol/pytest-plugins/tree/main/pytest-saas/>`_.  This
plugin makes additional fixtures available that are used in the saas
integration tests of BFSPY, see files in folder `test_saas/integration
<https://github.com/exasol/bucketfs-python/tree/main/test_saas/integration/>`_.

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
<https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment#required-reviewers>`_
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

Creating a Release
*******************

Prepare the Release
-------------------

To prepare for a release, a pull request with the following parameters needs to be created:

- Updated version numbers
- Updated the changelog
- Updated workflow templates (not automated yet)

This can be achieved by running the following command:

.. code-block:: shell

   nox -s prepare-release -- <major>.<minor>.<patch>

Replace `<major>`, `<minor>`, and `<patch>` with the appropriate version numbers.
Once the PR is successfully merged, the release can be triggered (see next section).

Triggering the Release
----------------------

.. warning::

    Before creating a tag for a release, make sure you have pulled in the latest changes
    from **master/main** (:code:`git pull`).

To trigger a release, a new tag must be pushed to GitHub. For further details, see `.github/workflows/ci-cd.yml`.

1. Create a local tag with the appropriate version number:

    .. code-block:: shell

        git tag x.y.z

2. Push the tag to GitHub:

    .. code-block:: shell

        git push origin x.y.z


What to do if the release failed?
---------------------------------

The release failed during pre-release checks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. Delete the local tag

    .. code-block:: shell

        git tag -d x.y.z

#. Delete the remote tag

    .. code-block:: shell

        git push --delete origin x.y.z

#. Fix the issue(s) which lead to the failing checks
#. Start the release process from the beginning


One of the release steps failed (Partial Release)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#. Check the Github action/workflow to see which steps failed
#. Finish or redo the failed release steps manually

.. note:: Example

    **Scenario**: Publishing of the release on Github was successfully but during the PyPi release, the upload step got interrupted.

    **Solution**: Manually push the package to PyPi

