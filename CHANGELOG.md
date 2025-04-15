# Change log

lsst-tap-service is versioned with [semver](https://semver.org/). Dependencies are updated to the latest available version during each release. Those changes are not noted here explicitly.

Find changes for the upcoming release in the project's [changelog.d](https://github.com/lsst-sqre/lsst-tap-service/tree/main/changelog.d/).

<!-- scriv-insert-here -->

<a id='changelog-2.13.0'></a>
## 2.13.0 (2025-04-14)

### Changed

- Remove unneeded implementations (Registry implementation, now fixed in cadc-reg 1.4.3). (To address bug where we send out requests to the cadc reg)
- Remove JobDAO and CachingFile implementations (JobDAO bug has been fixed, and these were retained here because of the log4j checkpoint bug, i.e. getting checkpoint logs due to log4j conflicts)
- Fix log4j / log4j2 issues (cadc-util imports the log4j2 apis so we don't need to add the dependency). We just include the log4j2 configuration here


<a id='changelog-2.12.0'></a>
## 2.12.0 (2025-04-04)

### Changed

- Upgrade cadc packaged

### Removed

- Removed ALMATableServlet and MaxRecValidatorImpl implementations

<a id='changelog-2.11.0'></a>
## 2.11.0 (2025-02-27)


### Added

- log4j xml config required by cadc-log package

### Changed

- Configure logging for both log4j (cadc-log) and log4j2 needed for Sentry

- Don't log user errors in Sentry

<a id='changelog-2.10.0'></a>
## 2.10.0 (2025-02-26)

### Changed

- Use cadc-adql version 1.1.4
- Upgrade to jre 11

<a id='changelog-2.9.0'></a>
## 2.9.0 (2025-02-20)

### Added

- Enable Sentry error logging and tracing for the Query Runner

<a id='changelog-2.8.0'></a>
## 2.8.0 (2025-02-19)

### Added

- New /reg endpoint for service resource-caps. Removes dependency on external ws by cadc

### Fixed

- Fix Rubin Table writes to work with newer version of cadc-dali

<a id='changelog-2.7.0'></a>
## 2.7.0 (2025-02-05)

### Added

- ResultSetWriter class, used to write out Binary2 VOTable serialization

### Changed

- Added new Binary2 VOTable serialization, make default. Retain tabledata as an optional format

### Fixed

- Correctly print field metadata

<a id='changelog-2.6.0'></a>
## 2.6.0 (2025-01-30)

### Added

- JobDAO class from opencadc adapted to fix issue with LAST param in joblist

### Fixed

- Fixed broken maven build, remove unneeded repository info

<a id='changelog-2.5.0'></a>
## 2.5.0 (2024-11-01)

### Removed

- Update jcenter repo (obsolete)
- Remove  restlet jar

- Removed deprecated AuthenticatorImpl class

### Changed

- Removed extra spaces from dockerfile to adhere to best practices

- Updated version of gms to >=1.0.14

### Fixed

- docker-compose.yml to get updated CATALINA_OPTS required to run local TAP service instance

- Label warning in Docker build

<a id='changelog-2.4.7'></a>
## 2.4.7 (2024-07-31)

### Changed

- Revert uws connection pool settings to release 2.3.0

<a id='changelog-2.4.6'></a>
## 2.4.6 (2024-07-30)

### Changed

- Added some cadc packages to log control & add missing header to log4j

<a id='changelog-2.4.5'></a>
## 2.4.5 (2024-07-24)

### Changed

- Changed QueryJobManager to use the IdentityManager available via the AuthenticationUtil class (OpenID in our case)
- Added deprecated AuthenticatorImpl, this is only useful in case this version of TAP is used with the old Auth params/implementations (Unlikely)
- Upgrade version of uws-server to 1.2.21

<a id='changelog-2.4.4'></a>
## 2.4.4 (2024-07-23)

### Changed

- Changed /tables endpoint accessURL to base, this allows us to add the 'lsst-token' to /tables as the security method

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

