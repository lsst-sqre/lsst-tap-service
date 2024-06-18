# Change log

lsst-tap-service is versioned with [semver](https://semver.org/). Dependencies are updated to the latest available version during each release. Those changes are not noted here explicitly.

Find changes for the upcoming release in the project's [changelog.d](https://github.com/lsst-sqre/lsst-tap-service/tree/main/changelog.d/).

<!-- scriv-insert-here -->

# 2024-06-18

## Other Changes

- Upgrade mysql-connector to 8.4.0

# 2024-06-11

## New features

- Added UWSInitAction class to initialise a UWS database
- Added scriv changelogs

## Other Changes

- Changed the build.gradle to use fixed version of the latest cadc libs
- Changed Dockerfile for lsst-tap-service to use cadc-tomcat base image
- Deprecated AuthenticatorImpl class

## Bug Fixes

- Fixed capabilities output (securityMethods)

