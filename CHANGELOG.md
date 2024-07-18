# Change log

lsst-tap-service is versioned with [semver](https://semver.org/). Dependencies are updated to the latest available version during each release. Those changes are not noted here explicitly.

Find changes for the upcoming release in the project's [changelog.d](https://github.com/lsst-sqre/lsst-tap-service/tree/main/changelog.d/).

<!-- scriv-insert-here -->

<a id='changelog-2.4.3'></a>
## 2.4.3 (2024-07-18)

### Fixed

- Start script now runs as ENTRYPOINT and that triggers the cadc start script.
- Fixes issue where Datalink manifest was not being fetched to /tmp

<a id='changelog-2.4.2'></a>
## 2.4.2 (2024-07-15)

### Changed

- Remove unneeded cadc-libs

### Fixed

- Conflict between stilts & dali packages

<a id='changelog-2.4.1'></a>
## 2.4.1 (2024-07-15)

### Changed

- Changed scriv settings to match tap-postgres
- Remove unneeded cadc dependencies (issue with dali/stilts conflict)
- Upgrade log4j (Log4j vulnerability)

<a id='changelog-2.4.0'></a>
# 2.4.0 (2024-06-28)

### Fixed

- Fixed Capabilities handling. Use new CapGetAction & CapInitAction, modified by getting pathPrefix from ENV property

## Other Changes

- Change result handling, to use a redirect servlet. Addresses issue with async failing due to auth header propagation with clients like pyvo, topcat

<a id='changelog-2.3.1'></a>
# 2.3.1 (2024-06-18)

## Other Changes

- Upgrade mysql-connector to 8.4.0

<a id='changelog-2.3.0'></a>
# 2.3.0 (2024-06-11)

## New features

- Added UWSInitAction class to initialise a UWS database
- Added scriv changelogs

## Other Changes

- Changed the build.gradle to use fixed version of the latest cadc libs
- Changed Dockerfile for lsst-tap-service to use cadc-tomcat base image
- Deprecated AuthenticatorImpl class

## Bug Fixes

- Fixed capabilities output (securityMethods)

