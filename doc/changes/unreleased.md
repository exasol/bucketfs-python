# Unreleased

## Summary

The current release adds a dependency to plugin `pytest_exasol_saas` and replaces individual test fixtures by those provided by the plugin.

Additionally the release fixes vulnerabilities by updating dependencies.

## Security

* Fixed vulnerabilities by updating dependencies
  * Vulnerability CVE-2024-21503 in transitive dependency via `exasol-toolbox` to `black` in versions below `24.3.0`
  * Vulnerability CVE-2024-35195 in dependency `requests` in versions below `2.32.0`

## Refactorings

* #141: Used plugin `pytest_exasol_saas`

## Documentation

* #144: Added comment on using fixtures from pytest-plugin `pytest-exasol-saas`
* #147: Added documentation for the SaaS and the PathLike interface.
