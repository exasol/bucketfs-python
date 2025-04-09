# Unreleased

# Security

* #193: Dependency updates, notably Jinja2 (3.1.4 -> 3.1.5), luigi (-> 3.6.0)
* #206: Dependency updates, notably cryptography(43.0.3->44.0.2), Jinja2 (3.1.5 -> 3.1.6)
  * Due to changes in cryptography's Python support (!=3.9.0 and 3.9.1), we updated our support to Python ^3.9.2.

# Bug fixing

* #202: Fixed the bug in the `Pathlike._navigate()` resulting in false positive `exists`.

## Refactorings

* #206: Updated to poetry 2.1.2 and exasol-toolbox to ^1.0.0