# 2.3.1 - 2026-10-23

## Summary

Fixed bugs in the `path` interface, in particular the path handling in Windows.

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| black | CVE-2026-114134 | 26.5.1 | 26.10.1 |
| cryptography | PYSEC-2026-3552 | 49.0.0 | 50.0.0 |
| gitpython | PYSEC-2026-3783 | 3.1.57 | 3.1.58 |
| gitpython | PYSEC-2026-3784 | 3.1.57 | 3.1.58 |
| gitpython | PYSEC-2026-3785 | 3.1.57 | 3.1.59 |
| gitpython | PYSEC-2026-3786 | 3.1.57 | 3.1.59 |
| gitpython | PYSEC-2026-3787 | 3.1.57 | 3.1.59 |
| gitpython | PYSEC-2026-3788 | 3.1.57 | 3.1.59 |
| gitpython | PYSEC-2026-3837 | 3.1.57 | 3.1.59 |
| gitpython | PYSEC-2026-3838 | 3.1.57 | 3.1.58 |
| gitpython | PYSEC-2026-3840 | 3.1.57 | 3.1.58 |
| gitpython | PYSEC-2026-3841 | 3.1.57 | 3.1.58 |
| gitpython | PYSEC-2026-3843 | 3.1.57 | 3.1.58 |
| gitpython | PYSEC-2026-3982 | 3.1.57 | 3.1.60 |
| gitpython | PYSEC-2026-3984 | 3.1.57 | 3.1.60 |
| pip | PYSEC-2026-3721 | 26.1.2 | 26.2 |
| tornado | PYSEC-2026-3928 | 6.5.7 | 6.5.8 |
| tornado | GHSA-chx6-46f5-w4vp | 6.5.7 | 6.5.9 |
| tornado | GHSA-c2m8-h5v5-343r | 6.5.7 | 6.5.9 |
| tornado | GHSA-3hv7-mjh2-fv65 | 6.5.7 | 6.5.9 |
| tornado | GHSA-wwv5-g3v4-889x | 6.5.7 | 6.5.8 |
| tornado | GHSA-8423-8fgw-73vq | 6.5.7 | 6.5.8 |
| urllib3 | PYSEC-2026-4177 | 2.7.0 | 2.8.0 |
| urllib3 | PYSEC-2026-4176 | 2.7.0 | 2.8.0 |
| urllib3 | PYSEC-2026-4175 | 2.7.0 | 2.8.0 |
| virtualenv | PYSEC-2026-4011 | 21.7.0 | 21.7.12 |
| virtualenv | PYSEC-2026-4012 | 21.7.0 | 21.7.11 |
| virtualenv | PYSEC-2026-4013 | 21.7.0 | 21.7.13 |
| virtualenv | PYSEC-2026-4014 | 21.7.0 | 21.7.12 |
| virtualenv | CVE-2026-113726 | 21.7.0 | 21.14.4 |
| virtualenv | CVE-2026-113732 | 21.7.0 | 21.14.2 |
| virtualenv | CVE-2026-113725 | 21.7.0 | 21.14.7 |
| werkzeug | PYSEC-2026-4312 | 3.1.8 | 3.1.9 |

## Bugfixes

* #322: Fixed `BucketPath` and `MountedBucket` using backslash separators on Windows; BucketFS paths are now always POSIX paths
* #329: Fixed absolute `BucketPath`, e.g. `'/dir/a.txt'`, being passed to the bucket with the leading `/` in data changing functions

## Dependency Updates

### `dev`

* Updated dependency `exasol-toolbox:10.4.0` to `10.5.0`
* Updated dependency `pyexasol:2.3.0` to `2.4.1`
