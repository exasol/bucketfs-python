# 1.1.0 - 2025-06-12

# Security

* #193: Dependency updates, notably Jinja2 (3.1.4 -> 3.1.5), luigi (-> 3.6.0)
* #206: Dependency updates, notably cryptography(43.0.3->44.0.2), Jinja2 (3.1.5 -> 3.1.6)
  * Due to changes in cryptography's Python support (!=3.9.0 and 3.9.1), we updated our support to Python ^3.9.2.
* #212: Dependency updates, notably h11(0.14.0->0.16.0)
* #217: Dependency updates, notably setuptools (78.1.0 -> 80.9.0), tornado (6.4.2 -> 6.5.1)
* #223: Dependency updates, notably requests (2.24.0 -> 2.32.4)

# Bug fixing

* #202: Fixed the bug in the `Pathlike._navigate()` resulting in false positive `exists`.
* #204: Fixed the automatic decompression when downloading a tar.gz file.

## Refactorings

* #206: Updated to poetry 2.1.2 and exasol-toolbox to ^1.0.0
* #223: Updated to exasol-toolbox ^1.4.0
* #220: Updated exasol-saas-api (0.7.0 -> 1.1.1) and dropped support for Python 3.9
