**************************
Building the Documentation
**************************

We are using the `Sphinx-GitHub_Pages_Generator <https://github.com/exasol/sphinx-github-pages-generator>`_
for generating the documentation of this project. It uses Sphinx and Git to make automation of building and providing
the documentation easier. Sphinx is the default documentation tool for Python projects,
and supports reStructuredText with proper cross-document references.



######################################################
Building the Documentation interactively during coding
######################################################

We defined several nox commands in the noxfile.py
which allow you to build and view the documentation during coding::

.. code:: bash

    nox -s build-html-doc # Builds the documentation
    nox -s open-html-doc # Opens the currently build documentation in the browser
    nox -s build-and-open-html-doc # Builds and opens the documentation

All three build commands generate the documentation into /doc/.build-docu
which is excluded in gitignore.

####################################
Building the Documentation in the CI
####################################

Building the documentation in the CI is a bit different to the steps you can use during coding,
because it also contains the preparations for publishing. At the moment, we publish
the documentation on Github Pages.

To publish it there, we need to build the HTML from the documentation source and commit it, as well as make sure we
adhere to the file structure expected by GitHub Pages.This is done using the
`Sphinx-GitHub_Pages_Generator <https://github.com/exasol/sphinx-github-pages-generator>`_

This has the additional benefit, that we don't have automatic commits to the source branch.
For each branch or tag for which we build the documentation in the CI, a directory is added to the root
directory of the github-pages/main branch.

With each merge into the main branch the CI updates the documentation for the main branch in github-pages/main.
For feature branches the CI checks this deployment process by creating a branch github-pages/<feature-branch-name>,
but it removes the branch directly after pushing it.
On tag creation, the documentation is also build, and saved in the "tag-name" directory. This is helpfully for releases.

You can run these tasks manually for testing purposes or
checking the branch with Github Pages in a fork of the main repository.
For this purpose, we also provide a few shortcuts defined in our noxfile.py::

.. code:: bash

    nox -s commit-pages-main  # creates or updates github-pages/main locally
    nox -s push-pages-main  # creates or updates github-pages/main and pushes it to origin
    nox -s commit-pages-current  # creates or updates github-pages/<current-branch-name> locally
    nox -s push-pages-current  # creates or updates github-pages/<current-branch-name> and pushes it to origin
    nox -s push-pages-release  # creates or updates github-pages/<latest-tag> and pushes it to origin

