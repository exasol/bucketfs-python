**************************
Building the Documentation
**************************

We are using Sphinx for generating the documentation of this project,
because it is the default documentation tool for Python projects.
Sphinx supports API documentation generation for Python and with plugins also for other languages.
Furthermore, it supports reStructuredText with proper cross-document references.
We using the MyST-Parser to also integrate markdown files into the documentation.

######################################################
Building the Documentation interactivily during coding
######################################################

We defined several commands in the project.toml in poethepoet
which allow you to build and view the documentation during coding::

    poetry run poe build-html-doc # Builds the documentation
    poetry run poe open-html-doc # Opens the currently build documentation in the browser
    poetry run poe build-and-open-html-doc # Builds and opens the documentation

All three build commands use the generated documentation located in /doc/_build/
which is excluded in gitignore. If you want to build the documentation for other formats than HTML,
you find a Makefile in /doc which allows you to run the sphinx build with other goals.

####################################
Building the Documentation in the CI
####################################

Building the documentation in the CI is a bit different to the steps you can use during coding,
because it also contains the preparations for publishing. At the moment, we publish
the documentation on Github Pages.

To publish it there, we need to build the HTML from the documentation source and commit it.
However, Github Pages expects a specific directory structure to find the HTML.
Our usual directory structure doesn't fit these requirements, so we decided to create
a new Git root commit and initially set github-pages/main branch to this commit.
We then add new commits to this branch to update existing or add new versions of the documentation.

This has the additional benefit, that we don't have automatic commits to the source branch.
For each branch or tag for which we build the documentation in the CI
we add a directory to the root directory of the github-pages/main branch.

With each merge into the main branch the CI updates the documentation for the main branch in github-pages/main.
For feature branches the CI checks this deployment process by creating a branch github-pages/<feature-branch-name>.
but it removes the branch directly after pushing it. However, you can run this also locally for testing purposes or
checking the branch with Github Pages in a fork of the main repostory.
The scripts which are responsible for the deployment are::

    deploy-to-github-pages-current # creates or updates github-pages/<current-branch-name>
    deploy-to-github-pages-main.sh # only applicable for the main branch and creates or updates github-pages/main


We also provide a few shortcuts defined in our project.toml for poethepoet::

    poetry run poe commit-html-doc-to-github-pages-main # creates or updates github-pages/main locally
    poetry run poe push-html-doc-to-github-pages-main  # creates or updates github-pages/main and pushes it to origin
    poetry run poe commit-html-doc-to-github-pages-current  # creates or updates github-pages/<current-branch-name> locally
    poetry run poe push-html-doc-to-github-pages-current  # creates or updates github-pages/<current-branch-name> and pushes it to origin

