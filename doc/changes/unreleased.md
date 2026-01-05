# Unreleased

## Bug Fixing

* #262: Fixed a wrong type interpretation in `path.write`. The chunks are now passed as a Sequence, not Iterable.

## Internal

* #260: Re-locked transitive dependencies urllib3, filelock, and Werkzeug and update to exasol-toolbox 4.0.0